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
    return new OpLogMessage(op, data, zipClock(clockKeys, clockValues))
  }
}

export class OpLogMessage {
  constructor (op, data, clock) {
    this.op = op
    this.data = data
    this.clock = clock
  }

  get clockUnzipped () {
    return unzipClock(this.clock)
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

const IndexWrapperSchema = {
  preencode (state, wrapper) {
    c.buffer.preencode(state, wrapper.value)
    c.buffer.preencode(state, wrapper.writer)
    c.array(c.uint).preencode(state, wrapper.conflicts)
    const clock = wrapper.clockUnzipped
    c.array(c.buffer).preencode(state, clock.keys)
    c.array(c.uint).preencode(state, clock.values)
  },

  encode (state, wrapper) {
    c.buffer.encode(state, wrapper.value)
    c.buffer.encode(state, wrapper.writer)
    c.array(c.uint).encode(state, wrapper.conflicts)
    const clock = wrapper.clockUnzipped
    c.array(c.buffer).encode(state, clock.keys)
    c.array(c.uint).encode(state, clock.values)
  },

  decode (state) {
    const value = c.buffer.decode(state)
    const writer = c.buffer.decode(state)
    const conflicts = c.array(c.uint).decode(state)
    const clockKeys = c.array(c.buffer).decode(state)
    const clockValues = c.array(c.uint).decode(state)
    return new IndexWrapper(value, writer, conflicts, zipClock(clockKeys, clockValues))
  }
}

export class IndexWrapper {
  constructor (value, writer, conflicts, clock) {
    this.value = value
    this.writer = Buffer.isBuffer(writer) ? writer : Buffer.from(writer, 'hex')
    this.conflicts = conflicts
    this.clock = clock
  }

  get clockUnzipped () {
    return unzipClock(this.clock)
  }

  encode () {
    return IndexWrapper.encode(this)
  }

  static encode (wrapper) {
    return c.encode(IndexWrapperSchema, wrapper)
  }

  static decode (buf) {
    return c.decode(IndexWrapperSchema, buf)
  }
}

function unzipClock (obj) {
  const keys = [], values = []
  for (const k in obj) {
    keys.push(Buffer.from(k, 'hex'))
    values.push(obj[k])
  }
  return {keys, values}
}

function zipClock (keys, values) {
  const obj = {}
  for (let i = 0; i < keys.length; i++) {
    obj[keys[i].toString('hex')] = values[i] || 0
  }
  return obj
}