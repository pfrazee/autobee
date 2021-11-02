import Corestore from 'corestore'
import ram from 'random-access-memory'
import Autobee from '../index.js'

const store = new Corestore(ram)
const writer1 = store.get({ name: 'writer1' })
const index = store.get({ name: 'index' })

await writer1.ready()

const autobee = new Autobee({inputs: [writer1], defaultInput: writer1, indexes: index})
await autobee.ready()

console.log('Writing a and b')
await autobee.put('a', {foo: 'bar'})
await autobee.put('b', {foo: 'baz'})
console.log('MERGED a=', await autobee.get('a'))
console.log('MERGED b=', await autobee.get('b'))
console.log('WRITER1 a=', await autobee.bee(writer1).get('a'))
console.log('WRITER1 b=', await autobee.bee(writer1).get('b'))
console.log('')

console.log('Reading as stream')
for await (const item of autobee.createReadStream()) {
  console.log('MERGED', item)
}
for await (const item of autobee.bee(writer1).createReadStream()) {
  console.log('WRITER1', item)
}
console.log('')

console.log('Overwriting a and b')
await autobee.put('b', {foo: 'BAZ'})
await autobee.put('a', {foo: 'BAR'})
console.log('MERGED a=', await autobee.get('a'))
console.log('MERGED b=', await autobee.get('b'))
console.log('WRITER1 a=', await autobee.bee(writer1).get('a'))
console.log('WRITER1 b=', await autobee.bee(writer1).get('b'))
console.log('')

console.log('Deleting a and b')
await autobee.del('a')
await autobee.del('b')
console.log('MERGED a=', await autobee.get('a'))
console.log('MERGED b=', await autobee.get('b'))
console.log('WRITER1 a=', await autobee.bee(writer1).get('a'))
console.log('WRITER1 b=', await autobee.bee(writer1).get('b'))
console.log('')
