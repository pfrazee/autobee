import Corestore from 'corestore'
import ram from 'random-access-memory'
import Autobee from '../index.js'

const store = new Corestore(ram)
const writer1 = store.get({ name: 'writer1' })
const writer2 = store.get({ name: 'writer2' })
const index = store.get({ name: 'index' })

await writer1.ready()
await writer2.ready()

const autobee = new Autobee({inputs: [writer1, writer2], defaultInput: writer1, indexes: index})
await autobee.ready()

console.log('Writing a and b as writer 1')
await autobee.put('a', {foo: 'writer1'})
await autobee.put('b', {foo: 'writer1'})
console.log('MERGED a=', await autobee.get('a'))
console.log('MERGED b=', await autobee.get('b'))
console.log('WRITER1 a=', await autobee.bee(writer1).get('a'))
console.log('WRITER1 b=', await autobee.bee(writer1).get('b'))
console.log('WRITER2 a=', await autobee.bee(writer2).get('a'))
console.log('WRITER2 b=', await autobee.bee(writer2).get('b'))
console.log('')

console.log('Writing a and b as writer 2')
await autobee.bee(writer2).put('a', {foo: 'writer2'})
await autobee.bee(writer2).put('b', {foo: 'writer2'})
console.log('MERGED a=', await autobee.get('a'))
console.log('MERGED b=', await autobee.get('b'))
console.log('WRITER1 a=', await autobee.bee(writer1).get('a'))
console.log('WRITER1 b=', await autobee.bee(writer1).get('b'))
console.log('WRITER2 a=', await autobee.bee(writer2).get('a'))
console.log('WRITER2 b=', await autobee.bee(writer2).get('b'))
console.log('')

console.log('Reading as stream')
for await (const item of autobee.createReadStream()) {
  console.log('MERGED', item)
}
for await (const item of autobee.bee(writer1).createReadStream()) {
  console.log('WRITER1', item)
}
for await (const item of autobee.bee(writer2).createReadStream()) {
  console.log('WRITER2', item)
}
console.log('')

console.log('Overwriting a and b, one with each writer')
await autobee.put('b', {foo: 'writer1'})
await autobee.bee(writer2).put('a', {foo: 'writer2'})
console.log('MERGED a=', await autobee.get('a'))
console.log('MERGED b=', await autobee.get('b'))
console.log('WRITER1 a=', await autobee.bee(writer1).get('a'))
console.log('WRITER1 b=', await autobee.bee(writer1).get('b'))
console.log('WRITER2 a=', await autobee.bee(writer2).get('a'))
console.log('WRITER2 b=', await autobee.bee(writer2).get('b'))
console.log('')

console.log('Deleting a and b, one with each writer')
await autobee.del('a')
await autobee.bee(writer2).del('b')
console.log('MERGED a=', await autobee.get('a'))
console.log('MERGED b=', await autobee.get('b'))
console.log('WRITER1 a=', await autobee.bee(writer1).get('a'))
console.log('WRITER1 b=', await autobee.bee(writer1).get('b'))
console.log('WRITER2 a=', await autobee.bee(writer2).get('a'))
console.log('WRITER2 b=', await autobee.bee(writer2).get('b'))
