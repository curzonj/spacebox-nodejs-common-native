'use strict';

var Q = require("q")
var C = require("spacebox-common")
var buildRedis = require('./main').buildRedis
var EventEmitter = require('events').EventEmitter
var merge = require('./state_merge')
var zlib = require('zlib')
var uuidGen = require('node-uuid')
var WTF = require('wtf-shim')

module.exports = function(logger) {
    var queue = [],
        worldPromises = [],
        completedTick = 0,
        worldLoaded = false,
        workerAlive = false,
        worldStateStorage = {}

    var sharedRedis
    function getSharedRedis() {
        if (sharedRedis === undefined)
            sharedRedis = buildRedis(logger)

        return sharedRedis
    }

    // This won't work for multiple subscriptions, but we currently
    // only have one.
    logger.measure('subscriptionQueueLength', 'gauge', function() {
        return queue.length
    })

    logger.measure({
        processRedisMessage: 'timer',
        tickDelay: 'histogram',
    })

    var self = {
        events: new EventEmitter(),
        wait_for_world: function(opts) {
            logger.info(opts, "waiting for world")

            return self.wait_for_world_fn(function (data) {
                return C.find(data, opts, false)
            })
        },
        wait_for_world_fn: function(fn) {
            var result = fn(worldStateStorage)

            if (result !== undefined && result !== false) {
                return Q(result)
            } else {
                var deferred = Q.defer()

                worldPromises.push({
                    fn: fn,
                    promise: deferred,
                    expires_at: Date.now() + 500, // 500ms timeout
                })

                return deferred.promise
            }
        },
        checkWorldPromises: function(msg, deleted) {
            if (worldPromises.length > 0) {
                var now = Date.now()
                msg.changes.forEach(function(state) {
                    var fake = {}
                    fake[state.key] = worldStateStorage[state.key] || deleted[state.key]

                    worldPromises.forEach(function(pair, i) {
                        var result = pair.fn(fake)
                        if (result !== undefined && result !== false) {
                            pair.promise.resolve(result)
                            worldPromises.splice(i, 1)
                        } else if (pair.expires_at <= now) {
                            pair.promise.reject('timeout')
                            worldPromises.splice(i, 1)
                        }
                    })
                })
            }
        },
        processMessage: function() {
            var process_t = WTF.trace.events.createScope('processMessage')
            var unzip_t = WTF.trace.events.createScope('processMessage:gunzip(int32 length)')
        
            return function() {
                var range = WTF.trace.beginTimeRange('processMessage')
                var scope = process_t()
                var timer = logger.processRedisMessage.start()

                Q.fcall(function() {
                    var content = queue.shift()
                    //logger.trace({ content_length: content.length }, 'gunzip message')

                    var gzip_scope = unzip_t(content.length)
                    return Q.nfcall(zlib.gunzip, content).
                    tap(function() {
                        WTF.trace.leaveScope(gzip_scope)
                    })
                }).then(function(decompressed) {
                    var msg = JSON.parse(decompressed)
                    return self.onWorldTick(msg)
                }).fin(function() {
                    timer.end()

                    if (queue.length > 0 && worldLoaded) {
                        process.nextTick(self.processMessage)
                    } else {
                        workerAlive = false
                    }

                    WTF.trace.endTimeRange(range)
                }).fail(function(e) {
                    logger.error({ err: e }, "failed to process redis message")
                }).done()

                WTF.trace.leaveScope(scope)
            }
        }(),
        onWorldTick: function(msg) {
            var merge_changes_t = WTF.trace.events.createScope('merge_changes')
            var world_tickers_t = WTF.trace.events.createScope('promise_world_tickers')
            var message_t = WTF.trace.events.createScope('subscribe:onMessage')


            return function(msg) {
                var scope
                var message_scope = message_t()
                var deleted = {}

                scope = merge_changes_t()
                var change_keys = Object.keys(msg.changes)
                change_keys.forEach(function(uuid) {
                    var patch = msg.changes[uuid]
                    logger.trace({ patch: patch }, "received")

                    if (patch.tombstone === true) {
                        var old = deleted[uuid] = worldStateStorage[uuid]
                        logger.trace({ deleted: old }, 'full tombstone object')
                    }

                    merge.apply(worldStateStorage, uuid, patch)
                })
                WTF.trace.leaveScope(scope)

                scope = world_tickers_t()
                try {
                    self.events.emit('worldtick', msg, deleted)
                } catch(e) {
                    logger.fatal({ err: e }, 'failed worktick event')
                }
                WTF.trace.leaveScope(scope)

                completedTick = msg.ts
                var delay = Date.now() - completedTick
                logger.tickDelay.update(delay)

                self.events.emit('tick', msg.ts, delay)
                WTF.trace.leaveScope(message_scope)
            }
        }(),
        loaded: function() {
            return worldLoaded
        },
        subscribe: function() {
            var received_t = WTF.trace.events.createInstance('redis#received_message')
            
            return function() {
                var redis = buildRedis(logger)

                redis.on("end", function () {
                    worldLoaded = false
                    worldStateStorage = {}
                    completedTick = 0

                    self.events.emit('worldreset')
                    // redis client will reconnect
                    // and that will trigger another
                    // worldload
                })

                redis.on("message", function(channel, blob) {
                    received_t()
                    queue.push(blob)

                    if (!workerAlive && worldLoaded) {
                        workerAlive = true
                        process.nextTick(self.processMessage)
                    }
                })

                redis.on('ready', function() {
                    redis.subscribe("worldstate")
                    logger.debug("subscribed to worldstate")
                    self.loadFromRedis()
                })
            }
        }(),
        loadFromRedis: function() {
            var redis = getSharedRedis()
            merge.loadFromRedis(redis, worldStateStorage).
            then(function() {
                worldLoaded = true
                if (queue.length > 0)
                    process.nextTick(self.processMessage)

                logger.debug("loaded world state")
                self.events.emit('worldloaded')
            }).done()
        },

        queueChangeIn: function(uuid, patch) {
            C.assertUUID(uuid)

            if (typeof patch !== 'object' || Object.keys(patch).length === 0)
                throw new Error("invalid patch")

            return getSharedRedis().rpush("commands", JSON.stringify({
                uuid: uuid,
                patch: patch
            }))
        },
        getP: function(uuid) {
            var response = uuidGen.v1()
            var redis = getSharedRedis()

            logger.trace({ responseKey: response, uuid: uuid }, 'making a redis request')

            return redis.rpush('requests', JSON.stringify({
                response: response,
                key: uuid
            })).then(function() {
                return redis.blpop(response, 0)
            }).then(function(result) {
                logger.trace({ responseKey: response, result: result[1].toString() }, 'redis request response')
                return JSON.parse(result[1].toString())
            })
        },
        get: function(uuid) {
            C.assertUUID(uuid)

            return worldStateStorage[uuid]
        },

        completedTick: function() {
            return completedTick
        },
        getAllKeys: function() {
            return worldStateStorage
        },
    }

    self.events.on('worldtick', self.checkWorldPromises)
    self.events.on('worldloaded', function() {
        worldPromises = []
    })

    return self
}
