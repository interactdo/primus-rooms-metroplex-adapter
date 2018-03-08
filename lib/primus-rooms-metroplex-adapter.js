const PrimusRoomsAdapter = require('primus-rooms-adapter')
const Redis = require('ioredis')
const { compact, chunk, flatten, uniq, without } = require('lodash')
const async = require('async')

const MAX_SPARK_FORWARDS_PER_BATCH = 50000

const DEFAULT_ROOM_REFRESH_INTERVAL = 300000 // metroplex interval default

// some libraries - namely engine.io - may take a while
// to determine that the connection is closed (ie. if the page is refreshed quickly)
// this number is set defensively high so that we can
// have a high guarantee that the client is actually disconnected
const DEFAULT_HEARTBEAT_PING_INTERVAL = 60000

// Key TTLs are a factor greater than amount of time we plan to refresh the key
const TTL_REFRESH_DRIFT_FACTOR = 1.2

const DEFAULT_KEYS_MATCH_SCAN_COUNT = 100
const MEMBERS_SSCAN_COUNT = 10000

module.exports = class PrimusRoomsMetroplexAdapter extends PrimusRoomsAdapter {
  constructor (redis, primus, opts = {}) {
    super(opts)

    if (redis instanceof Redis) {
      this.redis = redis
    } else {
      throw new Error('redis object is not an instance of ioredis')
    }

    if (primus) {
      this.primus = primus
    } else {
      throw new Error('primus object is not an instance of Primus')
    }

    if (!primus.metroplex) {
      throw new Error('PrimusRoomsMetroplexAdapter must be instantiated after metroplex')
    }

    this._namespace = opts.namespace || 'room_manager'
    this._identifier = opts.identifier || new Date().getTime()

    this._roomSparkSetTTLSeconds = Math.ceil(
      (this.primus.metroplex.interval || DEFAULT_ROOM_REFRESH_INTERVAL) / 1000 * TTL_REFRESH_DRIFT_FACTOR
    )
    this._sparkRoomSetTTLSeconds = Math.ceil(
      DEFAULT_HEARTBEAT_PING_INTERVAL / 1000 * TTL_REFRESH_DRIFT_FACTOR
    )
  }

  /**
  * Initializes redis key TTL refreshers
  */
  initialize () {
    this._initializeRoomSetTTLRefresher()
    this._initializeSparkSetsTTLRefresher()
  }

  /**
  * Adds a socket to a room
  * @param {String} id - Socket id
  * @param {String} room - Room name
  * @param {Function} callback - Callback
  */
  add (id, room, callback) {
    const roomSparkSetKey = this._roomSparkSetKey(room)
    const sparkRoomSetKey = this._sparkRoomSetKey(id)
    this.redis.multi()
      .sadd(roomSparkSetKey, id)
      .expire(roomSparkSetKey, this._roomSparkSetTTLSeconds)
      .sadd(sparkRoomSetKey, room)
      .expire(sparkRoomSetKey, this._sparkRoomSetTTLSeconds)
      .exec((err, results) => {
        if (err) return callback(err)
        this._validateMultiResults(results, err => callback(err))
      })
  }

  /**
  * Get rooms the socket is in or get all rooms if no socket ID is provided
  * returns Array of room names
  * @param {String} [id] Socket id
  * @param {Function} callback Callback
  */
  get (id, callback) {
    if (id) {
      this.redis.smembers(this._sparkRoomSetKey(id), callback)
    } else {
      this._keysMatchingPattern(`${this._namespace}:rooms:*:*`, (err, keys) => {
        if (err) return callback(err)

        const rooms = uniq(keys.map(key => {
          const parts = key.split(':')
          return parts[parts.length - 1]
        }))

        callback(null, rooms)
      })
    }
  }

  /**
  * Remove a socket from a room or all rooms if a room name is not passed.
  * @param {String} id - Socket id
  * @param {String} [room] - Room name
  * @param {Function} callback Callback
  */
  del (id, room, callback) {
    if (room) {
      this.redis.multi()
        .srem(this._roomSparkSetKey(room), id)
        .srem(this._sparkRoomSetKey(id), room)
        .exec((err, results) => {
          if (err) return callback(err)
          this._validateMultiResults(results, err => callback(err))
        })
    } else {
      this.redis.smembers(this._sparkRoomSetKey(id), (err, roomsForUser) => {
        if (err) return callback(err)

        const multi = this.redis.multi()

        multi.srem(this._roomSparkSetKey(room), id)

        roomsForUser.forEach(room => {
          multi.srem(this._roomSparkSetKey(room), id)
        })

        multi
          .del(this._sparkRoomSetKey(id))
          .exec((err, results) => {
            if (err) return callback(err)
            this._validateMultiResults(results, err => callback(err))
          })
      })
    }
  }

  /**
  * Broadcast a packet. If no rooms are provided, will broadcast to all rooms.
  * @param {*} data - Data to broadcast
  * @param {Object} [opts] - Broadcast options
  * @param {Array} [opts.except=[]] - Socket ids to exclude
  * @param {Array} [opts.rooms=[]] - List of rooms to broadcast to
  * @param {Function} [opts.transformer] - Message transformer
  * @param {Object} clients - Connected clients
  * @param {Function} [callback] - Optional callback
  */
  broadcast (data, opts, clients, callback) {
    opts = opts || {}
    opts.rooms = opts.rooms || []
    opts.except = opts.except || []
    opts.transformer = opts.transformer || (data => data[0])
    callback = callback || (err => err && console.error(err))

    const transformedData = opts.transformer(data)

    if (opts.rooms.length > 0) {
      async.waterfall([
        callback => {
          async.map(opts.rooms, this.clients.bind(this), (err, results) => {
            if (err) return callback(err)
            callback(null, flatten(results))
          })
        },

        (sparkIds, callback) => {
          const withoutExcluded = without(sparkIds, ...opts.except)
          this._sendToSparks(withoutExcluded, transformedData, callback)
        }
      ], callback)
    } else {
      // this is currently very inefficient for a large number of sparks,
      // as it simply scans all spark-room keys and parses the spark ids from the keys

      // ideally, we store set `room_manager:${namespace}_${identifier}:sparks` to quickly
      // look up all of the sparks across servers
      // however, this doesn't seem to get called in practice, so i'm avoiding over-optimizing.
      this._keysMatchingPattern(`${this._namespace}:sparks:*`, 5000, (err, keys) => {
        if (err) return
        const sparkIds = keys.map(key => key.substring(`${this._namespace}:sparks:`.length))
        this._sendToSparks(sparkIds, transformedData, callback)
      })
    }
  }

  /**
    * Get client ids connected to a room.
    * returns Array of spark ids
    * @param {String} room - Room name
    * @param {Function} callback - Callback
    */
  clients (room, callback) {
    async.waterfall([
      callback => {
        this._keysMatchingPattern(`${this._namespace}:rooms:*:${room}`, callback)
      },

      (roomKeys, callback) => {
        this._setMembersForKeys(roomKeys, callback)
      }
    ], callback)
  }

  /**
  * Remove all sockets from a room.
  * @param {String|Array} room - Room name
  * @param {Function} callback - Callback
  */
  empty (room, callback) {
    async.autoInject({
      roomKeys: callback => {
        this._keysMatchingPattern(`${this._namespace}:rooms:*:${room}`, callback)
      },

      sparkIds: (roomKeys, callback) => {
        this._setMembersForKeys(roomKeys, callback)
      },

      removeData: (roomKeys, sparkIds, callback) => {
        const multi = this.redis.multi().del(roomKeys)

        sparkIds.forEach(sparkId => {
          multi.srem(this._sparkRoomSetKey(sparkId), room)
        })

        multi.exec((err, results) => {
          if (err) return callback(err)
          this._validateMultiResults(results, err => callback(err))
        })
      }
    }, callback)
  }

  /**
  * Check if a room is empty.
  * returns `true` if the room is empty, else `false`
  * @param {String} room - Room name
  * @param {Function} callback - Callback
  */
  isEmpty (room, callback) {
    this._keysMatchingPattern(`${this._namespace}:rooms:*:${room}`, (err, roomKeys) => {
      if (err) return callback(err)
      callback(null, roomKeys.length === 0)
    })
  }

  /**
  * Reset the store. Will remove everything including all socket data from other adapter in the same cluster
  * @param {Function} callback - Callback
  */
  clear (callback) {
    this._keysMatchingPattern(`${this._namespace}:*`, (err, keys) => {
      if (err) return callback(err)
      this.redis.del(keys, err => callback(err))
    })
  }

  _initializeRoomSetTTLRefresher () {
    setInterval(() => {
      this._refreshRoomSetsTTL(this._roomSparkSetTTLSeconds, err => {
        if (err) console.error(new Error(`Error refreshing room->spark set TTL: ${err}`))
      })
    }, Math.floor(this._roomSparkSetTTLSeconds / TTL_REFRESH_DRIFT_FACTOR * 1000))
  }

  _refreshRoomSetsTTL (ttl, callback) {
    this._keysMatchingPattern(`${this._namespace}:rooms:${this._serverInstance}:*`, (err, roomKeys) => {
      if (err) return callback(err)

      const multi = this.redis.multi()
      roomKeys.forEach(roomKey => {
        multi.expire(roomKey, ttl)
      })
      multi.exec((err, results) => {
        if (err) return callback(err)
        this._validateMultiResults(results, err => callback(err))
      })
    })
  }

  _initializeSparkSetsTTLRefresher () {
    this.primus.on('connection', spark => {
      spark.on('heartbeat', () => {
        this.redis.expire(this._sparkRoomSetKey(spark.id), this._sparkRoomSetTTLSeconds, err => {
          if (err) console.error(new Error(`Error refreshing spark->room set TTL: ${err}`))
        })
      })
    })
  }

  get _serverInstance () {
    // primus.metroplex.address is set asynchronously,
    // which is why we can't access & set this value in the constructor

    // concatenate with unique identifier so server instances
    // are unique between crashes, preventing the re-usage
    // of past room lists
    if (!this._serverInstanceValue) {
      this._serverInstanceValue = `${this.primus.metroplex.address}_${this._identifier}`
    }

    return this._serverInstanceValue
  }

  _roomSparkSetKey (room) {
    return `${this._namespace}:rooms:${this._serverInstance}:${room}`
  }

  _sparkRoomSetKey (sparkId) {
    return `${this._namespace}:sparks:${sparkId}`
  }

  // redis recommends we use `scan` instead of `keys`
  // when iterating large sets of keys.
  // see https://redis.io/commands/scan
  _keysMatchingPattern (pattern, count, callback) {
    if (!callback) {
      callback = count
      count = DEFAULT_KEYS_MATCH_SCAN_COUNT
    }

    let cursor = 0
    const keys = []

    async.doWhilst(cb => {
      this.redis.scan(cursor, 'MATCH', pattern, 'COUNT', count, (err, result) => {
        if (err) return cb(err)
        const [newCursor, newKeys] = result
        cursor = newCursor
        keys.push(...newKeys)
        cb()
      })
    }, () => cursor !== '0', err => {
      if (err) return callback(err)
      callback(null, keys)
    })
  }

  // returns all members of n sets using sscan instead of ssmembers
  // useful in situations where a set can contain many members
  // and we want to avoid blocking redis, which is single-threaded
  // see https://redis.io/commands/scan
  _setMembersForKeys (keys, callback) {
    async.map(keys, (roomKey, callback) => {
      let cursor = 0
      const members = []

      async.doWhilst(cb => {
        this.redis.sscan(roomKey, cursor, 'COUNT', MEMBERS_SSCAN_COUNT, (err, result) => {
          if (err) return cb(err)
          const [newCursor, newMembers] = result
          cursor = newCursor
          members.push(...newMembers)
          cb()
        })
      }, () => cursor !== '0', err => {
        if (err) return callback(err)
        callback(null, members)
      })
    }, (err, memberGroups) => {
      if (err) return callback(err)
      callback(null, flatten(memberGroups))
    })
  }

  // redis commands grouped using `multi` or `pipeline`
  // may yield errors per result.
  // this helper checks each result for errors and combines
  // them into a single error that can be handled
  _validateMultiResults (results, callback) {
    const errors = results.map(result => result[0])

    if (errors.some(error => !!error)) {
      callback(new Error(`${compact(errors)}`))
    } else {
      callback()
    }
  }

  _sendToSparks (sparkIds, data, callback) {
    // primus.forward.sparks runs into memory issues when called
    // with too many spark ids. to avoid this, we call it
    // using sub-groups of spark ids
    const sparkIdChunks = chunk(sparkIds, MAX_SPARK_FORWARDS_PER_BATCH)
    async.each(sparkIdChunks, (sparkIds, callback) => {
      this.primus.forward.sparks(sparkIds, data, callback)
    }, callback)
  }
}
