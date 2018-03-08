const { describe, it, beforeEach } = require('mocha')
const { assert } = require('chai')
const { parallel } = require('async')
const Redis = require('ioredis')
const sinon = require('sinon')
const { times } = require('lodash')
const PrimusRoomsMetroplexAdapter = require('../lib/primus-rooms-metroplex-adapter')

const redis = new Redis()

const testNamespace = 'namespace'
const testAddress = 'http://10.0.2.15:8888'
const testDate = new Date().getTime()

describe('primus-rooms-metroplex-adapter', function () {
  let adapter, primus

  beforeEach(done => {
    redis.flushdb(done)
  })

  beforeEach(function () {
    primus = {
      options: {
        pingInterval: 30
      },
      metroplex: {
        address: testAddress,
        interval: 30
      },
      on: () => {},
      forward: {
        sparks: sinon.stub().yields()
      }
    }

    adapter = new PrimusRoomsMetroplexAdapter(redis, primus, {
      namespace: testNamespace,
      identifier: testDate
    })
  })

  describe('add', () => {
    it('adds to both registries and sets TTLs', done => {
      adapter.add('spark_id', 'some_room', err => {
        if (err) return done(err)
        parallel([
          done => {
            redis.smembers(`${testNamespace}:rooms:${testAddress}_${testDate}:some_room`, (err, members) => {
              if (err) return done(err)
              assert.deepEqual(members, ['spark_id'])
              done()
            })
          },
          done => {
            redis.ttl(`${testNamespace}:rooms:${testAddress}_${testDate}:some_room`, (err, ttl) => {
              if (err) return done(err)
              assert.isAbove(ttl, 0)
              done()
            })
          },
          done => {
            redis.smembers(`${testNamespace}:sparks:spark_id`, (err, members) => {
              if (err) return done(err)
              assert.deepEqual(members, ['some_room'])
              done()
            })
          },
          done => {
            redis.ttl(`${testNamespace}:sparks:spark_id`, (err, ttl) => {
              if (err) return done(err)
              assert.isAbove(ttl, 0)
              done()
            })
          }
        ], done)
      })
    })
  })

  describe('get', () => {
    it('finds the rooms that a given spark id is in', done => {
      redis.sadd(`${testNamespace}:sparks:spark_id`, 'room1', 'room2', err => {
        if (err) return done(err)
        adapter.get('spark_id', (err, rooms) => {
          if (err) return done(err)
          assert.deepEqual(rooms.sort(), ['room1', 'room2'])
          done()
        })
      })
    })

    it('finds all room names if no spark id is provided', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:server1:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:rooms:server2:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:rooms:server1:room2`, 'spark3', 'spark4')
        .exec(err => {
          if (err) return done(err)
          adapter.get(null, (err, rooms) => {
            if (err) return done(err)
            assert.deepEqual(rooms.sort(), ['room1', 'room2'])
            done()
          })
        })
    })
  })

  describe('del', () => {
    it('removes a spark from a room', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:${testAddress}_${testDate}:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:sparks:spark1`, 'room1', 'room2')
        .exec(err => {
          if (err) return done(err)
          adapter.del('spark1', 'room1', err => {
            if (err) return done(err)
            parallel([
              done => {
                redis.smembers(`${testNamespace}:rooms:${testAddress}_${testDate}:room1`, (err, sparks) => {
                  if (err) return done(err)
                  assert.deepEqual(sparks, ['spark2'])
                  done()
                })
              },
              done => {
                redis.smembers(`${testNamespace}:sparks:spark1`, (err, sparks) => {
                  if (err) return done(err)
                  assert.deepEqual(sparks, ['room2'])
                  done()
                })
              }
            ], done)
          })
        })
    })

    it('removes a spark from all rooms', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:${testAddress}_${testDate}:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:rooms:${testAddress}_${testDate}:room2`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:sparks:spark1`, 'room1', 'room2')
        .exec(err => {
          if (err) return done(err)
          adapter.del('spark1', null, err => {
            if (err) return done(err)
            parallel([
              done => {
                redis.smembers(`${testNamespace}:rooms:${testAddress}_${testDate}:room1`, (err, sparks) => {
                  if (err) return done(err)
                  assert.deepEqual(sparks, ['spark2'])
                  done()
                })
              },
              done => {
                redis.smembers(`${testNamespace}:rooms:${testAddress}_${testDate}:room2`, (err, sparks) => {
                  if (err) return done(err)
                  assert.deepEqual(sparks, ['spark2'])
                  done()
                })
              },
              done => {
                redis.smembers(`${testNamespace}:sparks:spark1`, (err, sparks) => {
                  if (err) return done(err)
                  assert.deepEqual(sparks, [])
                  done()
                })
              }
            ], done)
          })
        })
    })
  })

  describe('broadcast', () => {
    it('broadcasts the message to all sparks belonging to the room', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:server1:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:rooms:server2:room1`, 'spark3')
        .sadd(`${testNamespace}:rooms:server1:room2`, 'spark4')
        .sadd(`${testNamespace}:rooms:server1:room3`, 'spark5')
        .exec(err => {
          if (err) return done(err)

          const opts = {
            rooms: ['room1', 'room2'],
            transformer: data => `${data}_transformed`
          }

          adapter.broadcast('some_data', opts, [], err => {
            if (err) return done(err)
            sinon.assert.calledOnce(primus.forward.sparks)
            assert.deepEqual(primus.forward.sparks.lastCall.args[0].sort(), [
              'spark1', 'spark2', 'spark3', 'spark4'
            ])
            assert.equal(primus.forward.sparks.lastCall.args[1], 'some_data_transformed')
            done()
          })
        })
    })

    it('uses a default transformer if none is provided', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:server1:room1`, 'spark1')
        .exec(err => {
          if (err) return done(err)

          const opts = {
            rooms: ['room1']
          }

          adapter.broadcast(['some_data'], opts, [], err => {
            if (err) return done(err)
            sinon.assert.calledOnce(primus.forward.sparks)
            assert.equal(primus.forward.sparks.lastCall.args[1], 'some_data')
            done()
          })
        })
    })

    it('broadcasts to all sparks if no rooms are provided', done => {
      redis.multi()
        .sadd(`${testNamespace}:sparks:spark1`, 'room1', 'room2')
        .sadd(`${testNamespace}:sparks:spark2`, 'room3')
        .exec(err => {
          if (err) return done(err)

          adapter.broadcast(['some_data'], {}, [], err => {
            if (err) return done(err)
            sinon.assert.calledOnce(primus.forward.sparks)
            assert.deepEqual(primus.forward.sparks.lastCall.args[0].sort(), [
              'spark1', 'spark2'
            ])
            assert.equal(primus.forward.sparks.lastCall.args[1], 'some_data')
            done()
          })
        })
    })

    it('can take an array of sparks to disclude', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:server1:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:rooms:server2:room1`, 'spark3')
        .sadd(`${testNamespace}:rooms:server1:room2`, 'spark4')
        .sadd(`${testNamespace}:rooms:server1:room3`, 'spark5')
        .exec(err => {
          if (err) return done(err)

          const opts = {
            rooms: ['room1', 'room2'],
            except: ['spark2', 'spark3'],
            transformer: data => `${data}_transformed`
          }

          adapter.broadcast('some_data', opts, [], err => {
            if (err) return done(err)
            sinon.assert.calledOnce(primus.forward.sparks)
            assert.deepEqual(primus.forward.sparks.lastCall.args[0].sort(), [
              'spark1', 'spark4'
            ])
            assert.equal(primus.forward.sparks.lastCall.args[1], 'some_data_transformed')
            done()
          })
        })
    })
  })

  describe('clients', () => {
    it('gets the spark ids connected to a room across servers', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:server1:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:rooms:server2:room1`, 'spark3')
        .sadd(`${testNamespace}:rooms:server1:room2`, 'spark4')
        .exec(err => {
          if (err) return done(err)
          adapter.clients('room1', (err, clients) => {
            if (err) return done(err)
            assert.deepEqual(clients.sort(), ['spark1', 'spark2', 'spark3'])
            done()
          })
        })
    })
  })

  describe('empty', () => {
    it('deletes all room keys and removes the rooms from each spark array', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:server1:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:rooms:server2:room1`, 'spark3')
        .sadd(`${testNamespace}:rooms:server1:room2`, 'spark1', 'spark4')
        .sadd(`${testNamespace}:sparks:spark1`, 'room1', 'room2')
        .sadd(`${testNamespace}:sparks:spark2`, 'room1')
        .sadd(`${testNamespace}:sparks:spark3`, 'room1')
        .sadd(`${testNamespace}:sparks:spark4`, 'room2')
        .exec(err => {
          if (err) return done(err)
          adapter.empty('room1', err => {
            if (err) return done(err)
            redis.pipeline()
              .smembers(`${testNamespace}:rooms:server1:room1`)
              .smembers(`${testNamespace}:rooms:server2:room1`)
              .smembers(`${testNamespace}:rooms:server1:room2`)
              .smembers(`${testNamespace}:sparks:spark1`)
              .smembers(`${testNamespace}:sparks:spark2`)
              .smembers(`${testNamespace}:sparks:spark3`)
              .smembers(`${testNamespace}:sparks:spark4`)
              .exec((err, results) => {
                if (err) return done(err)
                const [
                  server1Room1Sparks,
                  server2Room1Sparks,
                  server1Room2Sparks,
                  spark1Rooms,
                  spark2Rooms,
                  spark3Rooms,
                  spark4Rooms
                ] = results

                assert.lengthOf(server1Room1Sparks[1], 0)
                assert.lengthOf(server2Room1Sparks[1], 0)
                assert.deepEqual(server1Room2Sparks[1].sort(), ['spark1', 'spark4'])
                assert.deepEqual(spark1Rooms[1], ['room2'])
                assert.lengthOf(spark2Rooms[1], 0)
                assert.lengthOf(spark3Rooms[1], 0)
                assert.deepEqual(spark4Rooms[1], ['room2'])
                done()
              })
          })
        })
    })
  })

  describe('isEmpty', () => {
    it('returns true if a room is empty - false otherwise', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:server1:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:rooms:server2:room1`, 'spark3')
        .exec(err => {
          if (err) return done(err)
          parallel([
            done => adapter.isEmpty('room1', (err, room1IsEmpty) => {
              if (err) return done(err)
              assert.isFalse(room1IsEmpty)
              done()
            }),
            done => adapter.isEmpty('room2', (err, room2IsEmpty) => {
              if (err) return done(err)
              assert.isTrue(room2IsEmpty)
              done()
            })
          ], done)
        })
    })
  })

  describe('clear', () => {
    it('clears all room data across servers', done => {
      redis.multi()
        .sadd(`${testNamespace}:rooms:server1:room1`, 'spark1', 'spark2')
        .sadd(`${testNamespace}:rooms:server2:room1`, 'spark3')
        .sadd(`${testNamespace}:sparks:spark1`, 'room1')
        .set('unrelatedData', 'value')
        .exec(err => {
          if (err) return done(err)
          adapter.clear(err => {
            if (err) return done(err)
            redis.keys('*', (err, keys) => {
              if (err) return done(err)
              assert.deepEqual(keys, ['unrelatedData'])
              done()
            })
          })
        })
    })
  })

  describe('_keysMatchingPattern', () => {
    it('finds all keys matching the given pattern', done => {
      // test a large number of keys to ensure that redis scan will iterate
      const numKeys = 100

      const keys = times(numKeys, n => `namespace:${n}`)
      const nonMatchingKeys = times(20, n => `wrong_namespace:${n}`)

      const addKeys = keys.concat(nonMatchingKeys).reduce((multi, key) => {
        return multi.set(key, 'value')
      }, redis.multi())

      addKeys.exec(err => {
        if (err) return done(err)
        adapter._keysMatchingPattern('namespace:*', 20, (err, returnedKeys) => {
          if (err) return done(err)
          assert.lengthOf(returnedKeys, numKeys)
          assert.sameMembers(returnedKeys, keys)
          done()
        })
      })
    })
  })
})
