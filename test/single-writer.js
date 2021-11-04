import ava from 'ava'
import Corestore from 'corestore'
import ram from 'random-access-memory'
import Autobee from '../index.js'

let store
let writer1
let index
let autobee

ava.before(async () => {
  store = new Corestore(ram)
  writer1 = store.get({ name: 'writer1' })
  index = store.get({ name: 'index' })
  await writer1.ready()
  
  autobee = new Autobee({inputs: [writer1], defaultInput: writer1, indexes: index})
  await autobee.ready()
})

ava.serial('Write, read, delete values', async (t) => {
  await autobee.put('a', 'foo')
  await autobee.put('b', 'bar')
  t.is((await autobee.get('a')).value, 'foo')
  t.is((await autobee.get('b')).value, 'bar')

  for await (const item of autobee.createReadStream()) {
    if (item.key === 'a') t.is(item.value, 'foo')
    if (item.key === 'b') t.is(item.value, 'bar')
  }

  await autobee.put('b', 'BAR')
  await autobee.put('a', 'FOO')
  t.is((await autobee.get('a')).value, 'FOO')
  t.is((await autobee.get('b')).value, 'BAR')

  await autobee.del('a')
  await autobee.del('b')
  t.is(await autobee.get('a'), null)
  t.is(await autobee.get('b'), null)
})

ava.serial('Write, read, delete sub() values', async (t) => {
  await autobee.sub('test').put('a', 'foo')
  await autobee.sub('test').put('b', 'bar')
  t.is((await autobee.sub('test').get('a')).value, 'foo')
  t.is((await autobee.sub('test').get('b')).value, 'bar')

  for await (const item of autobee.sub('test').createReadStream()) {
    if (item.key === 'a') t.is(item.value, 'foo')
    if (item.key === 'b') t.is(item.value, 'bar')
  }

  await autobee.sub('test').put('b', 'BAR')
  await autobee.sub('test').put('a', 'FOO')
  await autobee.sub('test2').put('a', 'another')
  await autobee.sub('test2').put('b', 'value')
  t.is((await autobee.sub('test').get('a')).value, 'FOO')
  t.is((await autobee.sub('test').get('b')).value, 'BAR')
  t.is((await autobee.sub('test2').get('a')).value, 'another')
  t.is((await autobee.sub('test2').get('b')).value, 'value')

  await autobee.sub('test').del('a')
  await autobee.sub('test').del('b')
  t.is(await autobee.sub('test').get('a'), null)
  t.is(await autobee.sub('test').get('b'), null)
  t.is((await autobee.sub('test2').get('a')).value, 'another')
  t.is((await autobee.sub('test2').get('b')).value, 'value')
})