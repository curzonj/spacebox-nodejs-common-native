'use strict';

var Q = require('q'),
    zlib = require('zlib'),
    deepMerge = require('spacebox-common/src/deepMerge')

module.exports = {
    apply: function(ws, uuid, patch) {
        if (patch.tombstone === true) {
            delete ws[uuid]
        } else {
            var old = ws[uuid]
            if (old === undefined) {
                if (!patch.type) {
                    console.log(uuid, patch, ws)
                    throw new Error("invalid patch")
                }

                old = ws[uuid] = {
                    uuid: uuid
                }
            }

            deepMerge(patch, old)
        }
    },
    loadFromRedis: function(redis, storage) {
        return redis.get("worldstate").
        then(function(data) {
            if (data === null)
                return null

            return Q.nfcall(zlib.gunzip, data)
        }).then(function(data) {
            if (data === null)
                return

            var obj = JSON.parse(data)
            deepMerge(obj, storage)
        })
    },
}
