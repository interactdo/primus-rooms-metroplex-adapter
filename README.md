# primus-rooms-metroplex-adapter

[![Build Status](https://travis-ci.org/thomasdashney/primus-rooms-metroplex-adapter.svg?branch=master)](https://travis-ci.org/thomasdashney/primus-rooms-metroplex-adapter)
[![NPM version](https://img.shields.io/npm/v/primus-rooms-metroplex-adapter.svg)](https://www.npmjs.com/package/primus-rooms-metroplex-adapter)


Adapter for [`primus-room`](https://github.com/cayasso/primus-rooms)

* Backed by redis
* Depends on [`metroplex`](https://github.com/primus/metroplex) & [`omega-supreme`](https://github.com/primus/omega-supreme/)
* Sets expired keys, so it can gracefully recover if a server goes down
* Uses `scan` and `sscan` to avoid blocking the server for large datasets

## Installation

```bash
npm install --save primus-rooms-metroplex-adapter
```

```js
const PrimusRoomsMetroplexAdapter = require('primus-rooms-metroplex-adapter')

// initialize primus with required plugins

const primus = new Primus(server, {
  transformer: 'engine.io',
  middleware: omegaSupremeRoomsMiddleware(),
  redis
})

primus.plugin('omega-supreme', omegaSupreme)
primus.plugin('metroplex', metroplex)
primus.plugin('rooms', primusRooms)

// configure & initialize adapter

const roomsAdapter = new PrimusRoomsMetroplexAdapter(redis, primus)
primus.adapter = roomsAdapter
primus._rooms.adapter = roomsAdapter // apparently a necessary hack
roomsAdapter.initialize()
```

## Redis Data Schema

| Key | Type | Values |
|---|---|---|
| `room_manager:rooms:$serverId_$instanceId:$room` | set | `$sparkId`

- Used for finding all of the sparks belonging to a room
- TTL is refreshed periodically, similar to `metroplex`'s expiration refresh algorithm
- Includes the `$serverId` so that it can expire if the server goes down

| Key | Type | Values |
|---|---|---|
| `room_manager:sparks:$sparkId` | set | `$roomId`

- Used for finding the rooms a given spark id belongs to
- TTL is refreshed each time a `heartbeat` event is received from a spark
