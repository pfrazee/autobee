# Autobee

**This is an experiment and should not be used yet!**

A multiwriter Hyperbee comprised of multiple input Hyperbees using Autobase and Hypercore 10 (all alpha software).

```js
import Corestore from 'corestore'
import ram from 'random-access-memory'
import Autobee from 'autobee' // not actually published

const store = new Corestore(ram)
const writer1 = store.get({ name: 'writer1' })
const writer2 = store.get({ name: 'writer2' })
const index = store.get({ name: 'index' })

await writer1.ready()
await writer2.ready()

const autobee = new Autobee({inputs: [writer1, writer2], defaultInput: writer1, indexes: index})
await autobee.ready()

await autobee.put('a', {foo: 'writer1'})
await autobee.put('b', {foo: 'writer1'})
await autobee.get('a') => // {value: 'writer1', ...}
await autobee.get('b') => // {value: 'writer1', ...}

await autobee.bee(writer2).put('a', {foo: 'writer2'})
await autobee.get('a') => // {value: 'writer2', ...}
await autobee.get('b') => // {value: 'writer1', ...}

await autobee.bee(writer2).del('a')
await autobee.del('a')
await autobee.get('a') => // null
await autobee.get('b') => // null
```

See the examples folder for more.

## License

MIT