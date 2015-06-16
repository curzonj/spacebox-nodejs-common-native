'use strict';

var Q = require("q"),
    C = require("spacebox-common"),
    buildRedis = require('./main').buildRedis,
    EventEmitter = require('events').EventEmitter,
    merge = require('./state_merge'),
    zlib = require('zlib'),
    WTF = require('wtf-shim')

var queue = [],
    completedTick = 0,
    worldLoaded = false,
    workerAlive = false,
    worldStateStorage = {}

var logger = C.logging.defaultCtx()

var sharedRedis
function getSharedRedis() {
    if (sharedRedis === undefined)
        sharedRedis = buildRedis()

    return sharedRedis
}

// This won't work for multiple subscriptions, but we currently
// only have one.
C.stats.define('subscriptionQueueLength', 'gauge', function() {
    return queue.length
})

C.stats.defineAll({
    processRedisMessage: 'timer',
    tickDelay: 'histogram',
})

var self = module.exports = {
    events: new EventEmitter(),
    waitForTick: function(ctx, ts, timeout) {
        ctx.debug({
            ts: ts, timeout: timeout, completedTick: completedTick, type: (typeof ts),
        }, 'waitForTick')

        if (typeof ts !== 'number')
            throw new Error("invalid timestamp")

        if (ts <= completedTick)
            return Q(null)

        if (timeout === undefined)
            timeout = 120 // ms

        var started_at = Date.now()
        return Q.Promise(function(resolve, reject) {
            function onTick(completedTick, delay) {
                if (!resolve)
                    return

                if (ts <= completedTick) {
                    resolve(delay)
                    reject = null
                } else {
                    self.events.once('tick', onTick)
                }
            }
           
            setTimeout(function() {
                if (!reject)
                    return

                reject('waitForTick timeout')
                resolve = null
            }, timeout)

            self.events.once('tick', onTick)
        }).then(function(delay) {
            ctx.debug({ ts: ts, delayed: delay, waited: Date.now() - started_at }, 'waitForTick:resolved')
        })
    },
    processMessage: function() {
        var process_t = WTF.trace.events.createScope('processMessage')
        var unzip_t = WTF.trace.events.createScope('processMessage:gunzip(int32 length)')
    
        return function() {
            var range = WTF.trace.beginTimeRange('processMessage')
            var scope = process_t()
            var timer = C.stats.processRedisMessage.start()

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
                C.logging.defaultCtx().error({ err: e }, "failed to process redis message")
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

            scope = merge_changes_t()
            Object.keys(msg.changes).forEach(function(uuid) {
                merge.apply(worldStateStorage, uuid, msg.changes[uuid])
            })
            WTF.trace.leaveScope(scope)

            scope = world_tickers_t()
            self.events.emit('worldtick', msg)
            WTF.trace.leaveScope(scope)

            completedTick = msg.ts
            var delay = Date.now() - completedTick
            C.stats.tickDelay.update(delay)

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
            var redis = buildRedis()

            redis.on("end", function () {
                worldLoaded = false
                worldStateStorage = {}
                completedTick = 0

                self.events.emit('worldreset')
                // redis client will reconnect
                // and that will trigger another
                // worldload
            })

            redis.on('ready', function() {
                self.loadFromRedis()
            })

            redis.on("message", function(channel, blob) {
                received_t()
                queue.push(blob)

                if (!workerAlive && worldLoaded) {
                    workerAlive = true
                    process.nextTick(self.processMessage)
                }
            })

            redis.subscribe("worldstate")
            logger.debug("subscribed to worldstate")
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
