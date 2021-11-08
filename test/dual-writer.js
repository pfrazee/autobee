import ava from 'ava'
import Corestore from 'corestore'
import ram from 'random-access-memory'
import Autobee from '../index.js'

let store
let writer1
let writer2
let index
let autobee

ava.before(async () => {
  store = new Corestore(ram)
  writer1 = store.get({ name: 'writer1' })
  writer2 = store.get({ name: 'writer2' })
  index = store.get({ name: 'index' })
  await writer1.ready()
  await writer2.ready()
  
  autobee = new Autobee({inputs: [writer1, writer2], defaultInput: writer1, indexes: index})
  await autobee.ready()
})

ava.serial('Write, read, delete values', async (t) => {
  await autobee.put('a', 'writer1')
  await autobee.put('b', 'writer1')
  t.is((await autobee.get('a')).value, 'writer1')
  t.is((await autobee.get('b')).value, 'writer1')

  for await (const item of autobee.createReadStream()) {
    if (item.key === 'a') t.is(item.value, 'writer1')
    if (item.key === 'b') t.is(item.value, 'writer1')
  }

  await autobee.put('a', 'writer2', {writer: writer2})
  await autobee.put('b', 'writer2', {writer: writer2})
  t.is((await autobee.get('a')).value, 'writer2')
  t.is((await autobee.get('b')).value, 'writer2')

  for await (const item of autobee.createReadStream()) {
    if (item.key === 'a') t.is(item.value, 'writer2')
    if (item.key === 'b') t.is(item.value, 'writer2')
  }

  await autobee.put('a', 'writer1', {writer: writer1})
  await autobee.put('b', 'writer2', {writer: writer2})
  t.is((await autobee.get('a')).value, 'writer1')
  t.is((await autobee.get('b')).value, 'writer2')

  for await (const item of autobee.createReadStream()) {
    if (item.key === 'a') t.is(item.value, 'writer1')
    if (item.key === 'b') t.is(item.value, 'writer2')
  }

  await autobee.del('a', {writer: writer1})
  await autobee.del('b', {writer: writer2})
  t.is(await autobee.get('a'), null)
  t.is(await autobee.get('b'), null)
})

ava.skip('Write, read, delete sub() values', async (t) => {
  await autobee.sub('test').put('a', 'writer1')
  await autobee.sub('test').put('b', 'writer1')
  t.is((await autobee.sub('test').get('a')).value, 'writer1')
  t.is((await autobee.sub('test').get('b')).value, 'writer1')

  for await (const item of autobee.sub('test').createReadStream()) {
    if (item.key === 'a') t.is(item.value, 'writer1')
    if (item.key === 'b') t.is(item.value, 'writer1')
  }

  await autobee.sub('test').put('a', 'writer2', {writer: writer2})
  await autobee.sub('test').put('b', 'writer2', {writer: writer2})
  t.is((await autobee.sub('test').get('a')).value, 'writer2')
  t.is((await autobee.sub('test').get('b')).value, 'writer2')

  for await (const item of autobee.sub('test').createReadStream()) {
    if (item.key === 'a') t.is(item.value, 'writer2')
    if (item.key === 'b') t.is(item.value, 'writer2')
  }

  await autobee.sub('test').put('a', 'writer1', {writer: writer1})
  await autobee.sub('test').put('b', 'writer2', {writer: writer2})
  t.is((await autobee.sub('test').get('a')).value, 'writer1')
  t.is((await autobee.sub('test').get('b')).value, 'writer2')

  for await (const item of autobee.sub('test').createReadStream()) {
    if (item.key === 'a') t.is(item.value, 'writer1')
    if (item.key === 'b') t.is(item.value, 'writer2')
  }

  await autobee.sub('test').del('a', {writer: writer1})
  await autobee.sub('test').del('b', {writer: writer2})
  t.is(await autobee.sub('test').get('a'), null)
  t.is(await autobee.sub('test').get('b'), null)
})