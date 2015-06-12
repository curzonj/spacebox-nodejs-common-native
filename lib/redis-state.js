'use strict';

var Q = require("q"),
    C = require("spacebox-common"),
    buildRedis = require('../main').buildRedis,
    redis = buildRedis(),
    merge = require('./state_merge')

var listeners = [],
    currentTick,
    worldStateStorage = {}

var self = module.exports = {
    subscribe: function(fn) {
        var redis = buildRedis(),
            logger = C.logging.create()

        redis.on("error", function (err) {
            logger.info({ err: err, scope: 'redis' }, "connection error")
        });

        redis.on("end", function () {
            logger.info({ scope: 'redis' }, "connection closed");
        });

        redis.on("ready", function () {
            logger.info({ scope: 'redis' }, "connection opened");
        });

        fn(redis)

        redis.subscribe("worldstate")
    },

    loadWorld: function() {
        var worldLoaded = Q.defer()

        self.subscribe(function(redis) {
            redis.on("message", function(channel, blob) {
                var msg = JSON.parse(blob)
                worldLoaded.promise.then(function() {
                    currentTick = msg.ts

                    Object.keys(msg.changes).forEach(function(uuid) {
                        merge.apply(worldStateStorage, uuid, msg.changes[uuid])
                    })

                    listeners.forEach(function(h) {
                        h.onWorldTick(msg)
                    })

                }).done()
            })
        })
    
        // we need to start receiving messages before we load state,
        // but we have to wait to apply them until afterwards
        return merge.loadFromRedis(redis, worldStateStorage).
        then(function() {
            worldLoaded.resolve()
        })
    },

    queueChangeIn: function(uuid, patch) {
        C.assertUUID(uuid)

        if (typeof patch !== 'object' || Object.keys(patch).length === 0)
            throw new Error("invalid patch")

        return redis.rpush("commands", JSON.stringify({
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
