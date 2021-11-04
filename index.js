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

    // TODO should this.index possibly come from indexes?
    this.index = this.autobase.createRebasedIndex({
      unwrap: true,
      apply: this._apply.bind(this)
    })

    this.indexBee = new Hyperbee(this.index, {
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

  get config () {
    return {
      inputs: this.autobase.inputs,
      defaultInput: this.autobase.defaultInput,
      defaultIndexes: this.defaultIndexes
    }
  }

  async get (...args) {
    return await this.indexBee.get(...args)
  }

  createReadStream (...args) {
    return this.indexBee.createReadStream(...args)
  }

  async put (key, value, opts) {
    const core = this.autobase.defaultInput
    if (opts?.prefix) key = `${opts.prefix}${key}`
    const op = OpLogMessage.put(
      Buffer.from(key, 'utf-8'),
      this._valueEncoder.encode(value),
      {} // CLOCK TODO
    )
    return await core.append(op.encode())
  }

  async del (key, opts) {
    const core = this.autobase.defaultInput
    if (opts?.prefix) key = `${opts.prefix}${key}`
    const op = OpLogMessage.del(
      Buffer.from(key, 'utf-8'),
      {} // CLOCK TODO
    )
    return await core.append(op.encode())
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
    console.log('apply hit')
    if (this.indexBee._feed.length === 0) {
      // TODO still needed?
      await this.indexBee._feed.append(HyperbeeMessages.Header.encode({
        protocol: 'hyperbee'
      }))
    }

    const b = this.indexBee.batch({ update: false })
    for (const node of batch) {
      console.debug('apply', node)
      let op = undefined
      try {
        op = OpLogMessage.decode(node)
      } catch (e) {
        // skip: not an op
        console.debug('Invalid op', e, node)
        continue
      }

      console.debug('apply', op)
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

        console.log(key, value)
        if (op.op === 'put') await b.put(key, value)
        else if (op.op === 'del') await b.del(key)
        
        this._clocks.set(key, {change: change.toString('hex'), seq: node.seq})
        console.log(pastClock, this._clocks.get(key), clocks.local)
        if (pastClock) {
          if (!clocks.local.has(pastClock.change) || clocks.local.get(pastClock.change) < pastClock.seq) {
            console.log('CONFLICT DETECTED', key)
          } else {
            console.log('CONFLICT RESOLVED', key)
          }
        }
      }
    }
    await b.flush()
  }
}
