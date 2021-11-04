import codecs from 'codecs'
import Autobase from 'autobase'
import Hyperbee from 'hyperbee'
import HyperbeeMessages from 'hyperbee/lib/messages.js'
import { OpLogMessage } from './messages.js'

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
      valueEncoding
    })

    this._clocks = new Map()
  }

  async ready () {
    await this.autobase.ready()
    await this.indexBee.ready()
  }

  get writable () {
    return !!this.autobase.inputs.find(core => core.writable)
  }

  async get (...args) {
    return await this.indexBee.get(...args)
  }

  createReadStream (...args) {
    return this.indexBee.createReadStream(...args)
  }

  async put (key, value, opts) {
    const core = getWriter(this, opts)
    if (opts?.prefix) key = `${opts.prefix}${key}`
    const op = OpLogMessage.put(
      Buffer.from(key, 'utf-8'),
      this._valueEncoder.encode(value),
      {} // CLOCK TODO
    )
    return await this.autobase.append(op.encode(), null, core)
  }

  async del (key, opts) {
    const core = getWriter(this, opts)
    if (opts?.prefix) key = `${opts.prefix}${key}`
    const op = OpLogMessage.del(
      Buffer.from(key, 'utf-8'),
      {} // CLOCK TODO
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
        console.debug('Invalid op', e, node)
        continue
      }

      // console.debug('OP', op)
      if (!op.op) {
        // skip: not an op
        console.debug('Invalid op', op)
        continue
      }

      // console.log('applying', node, clocks)
      // TODO: handle conflicts

      if (op.key) {
        const key = op.key.toString('utf-8')
        const value = op.value ? this._valueEncoder.decode(op.value) : undefined
        
        const pastClock = this._clocks.get(key)

        // console.debug(key, value)
        if (op.op === 'put') await b.put(key, value)
        else if (op.op === 'del') await b.del(key)
        
        this._clocks.set(key, {change: change.toString('hex'), seq: node.seq})
        // console.debug(pastClock, this._clocks.get(key), clocks.local)
        if (pastClock) {
          if (!clocks.local.has(pastClock.change) || clocks.local.get(pastClock.change) < pastClock.seq) {
            // console.debug('CONFLICT DETECTED', key)
          } else {
            // console.debug('CONFLICT RESOLVED', key)
          }
        }
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