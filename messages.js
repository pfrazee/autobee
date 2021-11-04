import c from 'compact-encoding'

const OpLogMessageSchema = {
  preencode (state, op) {
    c.string.preencode(state, op.op)
    c.array(c.buffer).preencode(state, op.data)
    const clock = op.clockUnzipped
    c.array(c.buffer).preencode(state, clock.keys)
    c.array(c.uint).preencode(state, clock.values)
  },

  encode (state, op) {
    c.string.encode(state, op.op)
    c.array(c.buffer).encode(state, op.data)
    const clock = op.clockUnzipped
    c.array(c.buffer).encode(state, clock.keys)
    c.array(c.uint).encode(state, clock.values)
  },

  decode (state) {
    const op = c.string.decode(state)
    const data = c.array(c.buffer).decode(state)
    const clockKeys = c.array(c.buffer).decode(state)
    const clockValues = c.array(c.uint).decode(state)
    return new OpLogMessage(op, data, zip(clockKeys, clockValues))
  }
}

export class OpLogMessage {
  constructor (op, data, clock) {
    this.op = op
    this.data = data
    this.clock = clock
  }

  get clockUnzipped () {
    return unzip(this.clock)
  }

  get key () {
    return this.data[0]
  }

  get value () {
    return this.data[1]
  }

  encode () {
    return OpLogMessage.encode(this)
  }

  static put (key, value, clock) {
    return new OpLogMessage('put', [key, value], clock)
  }

  static del (key, clock) {
    return new OpLogMessage('del', [key], clock)
  }

  static encode (op) {
    return c.encode(OpLogMessageSchema, op)
  }

  static decode (buf) {
    return c.decode(OpLogMessageSchema, buf)
  }
}

function unzip (obj) {
  const keys = [], values = []
  for (const k in obj) {
    keys.push(Buffer.from(k, 'hex'))
    values.push(obj[k])
  }
  return {keys, values}
}

function zip (keys, values) {
  const obj = {}
  for (let i = 0; i < keys.length; i++) {
    obj[keys[i].toString('hex')] = values[i] || 0
  }
  return obj
}