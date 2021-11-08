import codecs from 'codecs'
import Autobase from 'autobase'
import Hyperbee from 'hyperbee'
import HyperbeeMessages from 'hyperbee/lib/messages.js'
import through from 'through2'
import { OpLogMessage, IndexWrapper } from './messages.js'

export default class Autobee {
  constructor ({inputs, defaultInput, indexes, valueEncoding} = {}) {
    inputs = inputs || []
    valueEncoding = valueEncoding || 'json'
    this._valueEncoding = valueEncoding
    this._valueEncoder = codecs(valueEncoding)
    
    this.autobase = new Autobase(inputs, {indexes, input: defaultInput})

    const index = this.autobase.createRebasedIndex({
      unwrap: true,
      apply: this._apply.bind(this)
    })
    this.indexBee = new Hyperbee(index, {
      extension: false,
      keyEncoding: 'utf-8',
      valueEncoding: 'binary'
    })
  }

  async ready () {
    await this.autobase.ready()
    await this.indexBee.ready()
  }

  get writable () {
    return !!this.autobase.inputs.find(core => core.writable)
  }

  async get (key, opts) {
    if (opts?.valueEncoding) {
      delete opts.valueEncoding // TODO support custom encoding
    }
    const entry = await this.indexBee.get(key, opts)
    if (entry) {
      const wrapper = IndexWrapper.decode(entry.value)
      entry.value = wrapper.value ? this._valueEncoder.decode(wrapper.value) : undefined
    }
    return entry
  }

  async getConflicts (key) {
    const current = await getAllCurrent(this.indexBee, this.indexBee, key)
    if (current.length <= 1) return []
    for (const entry of current) {
      entry.value = entry.value ? this._valueEncoder.decode(entry.value) : undefined
    }
    return current.map(entry => entry.value)
  }

  createReadStream (opts) {
    if (opts?.valueEncoding) {
      delete opts.valueEncoding // TODO support custom encoding
    }
    const encoder = this._valueEncoder
    return this.indexBee.createReadStream(opts).pipe(through.obj(function (entry, enc, cb) {
      const wrapper = IndexWrapper.decode(entry.value)
      entry.value = wrapper.value ? encoder.decode(wrapper.value) : undefined
      this.push(entry)
      cb()
    }))
  }

  async put (key, value, opts) {
    const core = getWriter(this, opts)
    if (opts?.prefix) key = `${opts.prefix}${key}`
    const op = OpLogMessage.put(
      Buffer.from(key, 'utf-8'),
      this._valueEncoder.encode(value),
      await genClock(this, core)
    )
    return await this.autobase.append(op.encode(), null, core)
  }

  async del (key, opts) {
    const core = getWriter(this, opts)
    if (opts?.prefix) key = `${opts.prefix}${key}`
    const op = OpLogMessage.del(
      Buffer.from(key, 'utf-8'),
      await genClock(this, core)
    )
    return await this.autobase.append(op.encode(), null, core)
  }

  sub (prefix, opts) {
    const indexBeeSub = this.indexBee.sub(prefix, opts)

    let _prefix = prefix
    if (!_prefix.endsWith('\x00')) _prefix = `${_prefix}\x00`
    indexBeeSub.put = (key, value, opts) => {
      opts = opts || {}
      opts.prefix = _prefix
      return this.put(key, value, opts)
    }
    indexBeeSub.del = (key, opts) => {
      opts = opts || {}
      opts.prefix = _prefix
      return this.del(key, opts)
    }
    return indexBeeSub
  }

  async _apply (batch, clocks, change) {
    if (this.indexBee._feed.length === 0) {
      // HACK
      // when the indexBee is using the in-memory rebased core
      // (because it doesnt have one of its own, and is relying on a remote index)
      // it doesn't correctly write its header
      // so we do it here
      // -prf
      await this.indexBee._feed.append(HyperbeeMessages.Header.encode({
        protocol: 'hyperbee'
      }))
    }

    const b = this.indexBee.batch({ update: false })
    for (const node of batch) {
      let op = undefined
      try {
        op = OpLogMessage.decode(node.value)
      } catch (e) {
        // skip: not an op
        continue
      }

      // console.debug('OP', op)
      if (!op.op) {
        // skip: not an op
        continue
      }

      if (op.key && op.op === 'del') {
        const key = op.key.toString('utf-8')
        await b.del(key)
      } else if (op.key && op.op === 'put') {
        const key = op.key.toString('utf-8')
        // const value = op.value ? this._valueEncoder.decode(op.value) : undefined

        // TODO can this replace the recorded clock in the op?
        const DEBUG_opClock = Object.fromEntries(clocks.local)
        const changeStr = change.toString('hex')
        DEBUG_opClock[changeStr] = (DEBUG_opClock[changeStr] || 0) + 1

        let entries = await getAllCurrent(b, this.indexBee, key)
        entries = entries.filter(entry => !leftDominatesRight(DEBUG_opClock, entry.clock))
        // console.log('writing', key, 'conflicts=', entries.map(e => e.seq))
        
        await b.put(key, IndexWrapper.encode(new IndexWrapper(
          op.value,
          change,
          entries.map(entry => entry.seq),
          DEBUG_opClock
        )))
      }
    }
    await b.flush()
  }
}

function getWriter (autobee, opts) {
  let core
  if (opts?.writer) {
    if (opts.writer.key) {
      core = autobee.autobase.inputs.find(c => c.key.equals(opts.writer.key)) 
    } else if (Buffer.isBuffer(opts.writer)) {
      core = autobee.autobase.inputs.find(c => c.key.equals(opts.writer)) 
    }
  } else {
    core = autobee.autobase.defaultInput
  }
  if (!core) {
    throw new Error(`Not a writer: ${opts.writer}`)
  }
  if (!core.writable) {
    throw new Error(`Not writable: ${opts.writer || core}`)
  }
  return core
}

async function getAllCurrent (batch, bee, key) {
  const entries = []
  const currentEntry = await batch.get(key)
  if (currentEntry) {
    Object.assign(currentEntry, IndexWrapper.decode(currentEntry.value))
    entries.push(currentEntry)
    for (const seq of currentEntry.conflicts) {
      const entry = await bee.checkout(seq + 1).get(key)
      if (entry) {
        Object.assign(entry, IndexWrapper.decode(entry.value))
        entries.push(entry)
      }
    }
  }
  return entries
}

async function genClock (autobee, writer) {
  const keyStr = writer.key.toString('hex')
  const clock = Object.fromEntries(await autobee.autobase.latest())
  clock[keyStr] = (clock[keyStr] || 0) + 1
  return clock
}

function leftDominatesRight (left, right) {
  left = left || {}
  right = right || {}
  const keys = new Set(Object.keys(left).concat(Object.keys(right)))
  for (const k of keys) {
    const lv = left[k] || 0
    const rv = right[k] || 0
    if (lv < rv) return false
  }
  return true
}