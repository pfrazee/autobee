import ava from 'ava'
import Corestore from 'corestore'
import ram from 'random-access-memory'
import Autobee from '../index.js'

ava('Handle conflicts correctly', async (t) => {
  await testFork (t, 3)
})

async function testFork (t, numNodes) {
  const {nodes, heal} = await setup(numNodes)

  console.log('WRITERS', nodes[0].writers.map(w => w.key.toString('hex')))

  for (let i = 0; i < numNodes; i++) {
    await nodes[i].autobee.put('a', `writer${i}`, {writer: nodes[i].writers[i]})
    await nodes[i].autobee.put('b', `writer${i}`, {writer: nodes[i].writers[i]})
  }
  
  for (let i = 0; i < numNodes; i++) {
    // not connected yet, so the local view is our node's last write
    t.is((await nodes[i].autobee.get('a')).value, `writer${i}`)
    t.is((await nodes[i].autobee.get('b')).value, `writer${i}`)

    // and no conflicts
    t.deepEqual(await nodes[i].autobee.getConflicts('a'), [])
    t.deepEqual(await nodes[i].autobee.getConflicts('b'), [])
  }

  console.log('\nHEALING\n')
  heal()

  const conflictValue = []
  for (let i = 0; i < numNodes; i++) conflictValue.push(`writer${i}`)
  conflictValue.sort()

  for (let i = 0; i < numNodes; i++) {
    // connected now, so the "first" writer will win
    t.is((await nodes[i].autobee.get('a')).value, `writer0`)
    t.is((await nodes[i].autobee.get('b')).value, `writer0`)

    // and conflicts are stored
    t.deepEqual((await nodes[i].autobee.getConflicts('a')).sort(), conflictValue)
    t.deepEqual((await nodes[i].autobee.getConflicts('b')).sort(), conflictValue)
  }

  console.log('\nOVERWRITING\n')
  await nodes[1].autobee.put('b', 'writer1')

  for (let i = 0; i < numNodes; i++) {
    // merging write on b means node1 now wins for b
    t.is((await nodes[i].autobee.get('a')).value, `writer0`)
    t.is((await nodes[i].autobee.get('b')).value, `writer1`)

    // and no conflicts on b
    t.deepEqual((await nodes[i].autobee.getConflicts('a')).sort(), conflictValue)
    t.deepEqual(await nodes[i].autobee.getConflicts('b'), [])
  }

  await nodes[1].autobee.put('a', 'writer1')

  for (let i = 0; i < numNodes; i++) {
    // both values merged
    t.is((await nodes[i].autobee.get('a')).value, `writer1`)
    t.is((await nodes[i].autobee.get('b')).value, `writer1`)

    // and no conflicts on either
    t.deepEqual(await nodes[i].autobee.getConflicts('a'), [])
    t.deepEqual(await nodes[i].autobee.getConflicts('b'), [])
  }
}

async function setup (numNodes) {
  /*
  create a set of nodes, each with 1 writable "writer core"
  the first node will also have the writable "index core"
  each node will then get read-only instances of the other nodes' writer cores
  and every node but the first will get read-only instances of the first node's index core
  finally, create autobees for each node

  initially, these nodes will be disconnected from each other as each has their own corestore
  the heal() function causes them all to connect
  */

  // create the nodes
  const nodes = []
  for (let i = 0; i < numNodes; i++) {
    nodes.push({
      store: new Corestore(ram),
      writers: Array(numNodes),
      index: undefined,
      autobee: undefined
    })
  }

  // create the writable writer cores
  for (let i = 0; i < numNodes; i++) {
    nodes[i].writers[i] = nodes[i].store.get({name: `writer${i}`})
    await nodes[i].writers[i].ready()
  }

  // create the first node's index core
  nodes[0].index = nodes[0].store.get({name: 'index'})
  await nodes[0].index.ready()

  // create readonly instances of each writer core
  for (let i = 0; i < numNodes; i++) {
    for (let j = 0; j < numNodes; j++) {
      if (!nodes[i].writers[j]) {
        nodes[i].writers[j] = nodes[i].store.get(nodes[j].writers[j].key)
      }
    }
  }

  // create the readonly instances of the index core
  for (let i = 1; i < numNodes; i++) {
    nodes[i].index = nodes[i].store.get(nodes[0].index.key)
  }

  // create the autobees
  for (let i = 0; i < numNodes; i++) {
    nodes[i].autobee = new Autobee({inputs: nodes[i].writers, defaultInput: nodes[i].writers[i], indexes: nodes[i].index})
    await nodes[i].autobee.ready()
  }

  return {nodes, heal: () => {
    const doneMap = {}
    for (let i = 0; i < numNodes; i++) {
      for (let j = 0; j < numNodes; j++) {
        if (i === j) continue
        const key = `${Math.min(i, j)}:${Math.max(i, j)}`
        if (doneMap[key]) continue

        const s = nodes[i].store.replicate(true)
        s.pipe(nodes[j].store.replicate(false)).pipe(s)

        doneMap[key] = true
      }
    }
  }}
}

async function logall (prefix, src) {
  for await (const item of src.createReadStream()) {
    console.log(prefix, item)
  }
}