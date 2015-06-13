'use strict';

var Q = require("q"),
    C = require("spacebox-common"),
    buildRedis = require('../main').buildRedis,
    merge = require('./state_merge')

var listeners = [],
    currentTick,
    worldStateStorage = {}

var sharedRedis
function getSharedRedis() {
    if (sharedRedis === undefined)
        sharedRedis = buildRedis()

    return sharedRedis
}

// This won't work for multiple subscriptions, but we currently
// only have one.
var queue = []
C.stats.define('subscriptionQueueLength', 'gauge', function() {
    return queue.length
})

C.stats.defineAll({
    processRedisMessage: 'timer'
})

var self = module.exports = {
    subscribe: function(cb) {
        var redis = buildRedis()

        var workerAlive = false
        function processMessage() {
            var timer = C.stats.processRedisMessage.start()

            Q.fcall(function() {
                return cb(queue.shift())
            }).fin(function() {
                timer.end()

                if (queue.length > 0) {
                    //setTimeout(processMessage, 2)
                    process.nextTick(processMessage)
                } else {
                    workerAlive = false
                }
            }).fail(function(e) {
                C.logging.defaultCtx().error({ err: e }, "failed to process redis message")
            }).done()
        }

        redis.on("message", function(channel, blob) {
            queue.push(blob)

            if (!workerAlive) {
                workerAlive = true
                process.nextTick(processMessage)
                //setTimeout(processMessage, 2)
            }
        })

        redis.subscribe("worldstate")
    },

    loadWorld: function() {
        var worldLoaded = Q.defer()

        self.subscribe(function(blob) {
            var msg = JSON.parse(blob)
            return worldLoaded.promise.then(function() {
                currentTick = msg.ts

                Object.keys(msg.changes).forEach(function(uuid) {
                    merge.apply(worldStateStorage, uuid, msg.changes[uuid])
                })

                return Q.all(listeners.map(function(h) {
                    return h.onWorldTick(msg)
                }))
            })
        })
    
        // we need to start receiving messages before we load state,
        // but we have to wait to apply them until afterwards
        return merge.loadFromRedis(getSharedRedis(), worldStateStorage).
        then(function() {
            worldLoaded.resolve()
        })
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
    addListener: function(l) {
        listeners.push(l)
    },
    removeListener: function(l) {
        var index = listeners.indexOf(l)
        listeners.splice(index, 1)
    },

    currentTick: function() {
        return currentTick
    },
    getAllKeys: function() {
        return worldStateStorage
    },

}
