import ava from 'ava'
import Corestore from 'corestore'
import ram from 'random-access-memory'
import Autobee from '../index.js'

ava('Handle conflicts correctly', async (t) => {
  for (let i = 2; i < 6; i++) {
    for (let j = 2; j < 5; j++) {
      console.log(`disjointThenFullyHealed( nodes=${i} writes=${j} )`)
      await disjointThenFullyHealed (t, i, j)
    }
  }
  for (let i = 2; i < 6; i++) {
    for (let j = 2; j < 5; j++) {
      console.log(`disjointThenPartiallyHealed( nodes=${i} writes=${j} )`)
      await disjointThenPartiallyHealed (t, i, j)
    }
  }
})

async function disjointThenPartiallyHealed (t, numNodes, numWrites) {
  const {nodes, heal} = await setup(numNodes)

  for (let i = 0; i < numNodes; i++) {
    for (let j = 0; j < numWrites; j++) {
      await nodes[i].autobee.put(''+j, `writer${i}`, {writer: nodes[i].writers[i]})
    }
  }
  
  for (let i = 0; i < numNodes; i++) {
    for (let j = 0; j < numWrites; j++) {
      // not connected yet, so the local view is our node's last write
      t.is((await nodes[i].autobee.get(''+j)).value, `writer${i}`)
      // and no conflicts
      t.deepEqual(await nodes[i].autobee.getConflicts(''+j), [])
    }
  }

  
  for (let numHealed = 2; numHealed <= numNodes; numHealed++) {
    heal(numHealed)

    const conflictValue = numHealed === 2 ? [`writer0`, `writer1`] : [`writer1`, `writer${numHealed - 1}`]

    for (let i = 0; i < numNodes; i++) {
      for (let j = 0; j < numWrites; j++) {
        if (i >= numHealed) {
          // not yet connected, still only the local view with no conflicts
          t.is((await nodes[i].autobee.get(''+j)).value, `writer${i}`, `unhealed read | numHealed=${numHealed} node${i} write${j} numNodes=${numNodes} numWrites=${numWrites}`)
          t.deepEqual(await nodes[i].autobee.getConflicts(''+j), [], `unhealed conflicts | numHealed=${numHealed} node${i} write${j} numNodes=${numNodes} numWrites=${numWrites}`)
        } else {
          // connected but not merged, so conflicts will be stored
          t.deepEqual((await nodes[i].autobee.getConflicts(''+j)).sort(), conflictValue, `healed conflicts not merged | numHealed=${numHealed} node${i} write${j} numNodes=${numNodes} numWrites=${numWrites}`)
        }
      }
    }

    for (let j = 0; j < numWrites; j++) {
      await nodes[1].autobee.put(''+j, 'writer1')

      for (let i = 0; i < numNodes; i++) {
        for (let k = 0; k < numWrites; k++) {
          if (i >= numHealed) {
            // not yet connected, still only the local view with no conflicts
            t.is((await nodes[i].autobee.get(''+j)).value, `writer${i}`)
            t.deepEqual(await nodes[i].autobee.getConflicts(''+j), [])
          } else if (k > j) {
            // still in conflict because no merge-write has occurred
            t.deepEqual((await nodes[i].autobee.getConflicts(''+k)).sort(), conflictValue)
          } else {
            // merging-write with connected nodes means node1 now wins
            t.is((await nodes[i].autobee.get(''+k)).value, `writer1`)
            t.deepEqual(await nodes[i].autobee.getConflicts(''+k), [])
          }
        }
      }
    }
  }
}

async function disjointThenFullyHealed (t, numNodes, numWrites) {
  const {nodes, heal} = await setup(numNodes)

  for (let i = 0; i < numNodes; i++) {
    for (let j = 0; j < numWrites; j++) {
      await nodes[i].autobee.put(''+j, `writer${i}`, {writer: nodes[i].writers[i]})
    }
  }
  
  for (let i = 0; i < numNodes; i++) {
    for (let j = 0; j < numWrites; j++) {
      // not connected yet, so the local view is our node's last write
      t.is((await nodes[i].autobee.get(''+j)).value, `writer${i}`, `unhealed read | node${i} write${j} numNodes=${numNodes} numWrites=${numWrites}`)
      // and no conflicts
      t.deepEqual(await nodes[i].autobee.getConflicts(''+j), [], `unhealed conflicts | node${i} write${j} numNodes=${numNodes} numWrites=${numWrites}`)
    }
  }

  heal()

  const conflictValue = []
  for (let i = 0; i < numNodes; i++) conflictValue.push(`writer${i}`)
  conflictValue.sort()

  for (let i = 0; i < numNodes; i++) {
    for (let j = 0; j < numWrites; j++) {
      // connected now, so the "first" writer will win
      t.is((await nodes[i].autobee.get(''+j)).value, `writer0`, `healed read not merged | node${i} write${j} numNodes=${numNodes} numWrites=${numWrites}`)
      // and conflicts are stored
      t.deepEqual((await nodes[i].autobee.getConflicts(''+j)).sort(), conflictValue, `healed conflicts not merged | node${i} write${j} numNodes=${numNodes} numWrites=${numWrites}`)
    }
  }

  for (let j = 0; j < numWrites; j++) {
    await nodes[1].autobee.put(''+j, 'writer1')

    for (let i = 0; i < numNodes; i++) {
      for (let k = 0; k < numWrites; k++) {
        if (k <= j) {
          // merging write means node1 now wins
          t.is((await nodes[i].autobee.get(''+k)).value, `writer1`, `healed merged read | node${i} write${j} read${k} numNodes=${numNodes} numWrites=${numWrites}`)
          t.deepEqual(await nodes[i].autobee.getConflicts(''+k), [], `healed merged conflicts | node${i} write${j} read${k} numNodes=${numNodes} numWrites=${numWrites}`)
        } else {
          // still in conflict because not written yet
          t.is((await nodes[i].autobee.get(''+k)).value, `writer0`, `healed read not merged | node${i} write${j} read${k}`)
          t.deepEqual((await nodes[i].autobee.getConflicts(''+k)).sort(), conflictValue,  `healed conflicts not merged | node${i} write${j} read${k} numNodes=${numNodes} numWrites=${numWrites}`)
        }
      }
    }
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
  the heal(n) function causes the first n nodes to connect
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

  const connections = new Set()
  return {nodes, heal: (n = numNodes) => {
    for (let i = 0; i < n; i++) {
      for (let j = 0; j < n; j++) {
        if (i === j) continue
        const key = `${Math.min(i, j)}:${Math.max(i, j)}`
        if (connections.has(key)) continue

        const s = nodes[i].store.replicate(true)
        s.pipe(nodes[j].store.replicate(false)).pipe(s)

        connections.add(key)
      }
    }
  }}
}
