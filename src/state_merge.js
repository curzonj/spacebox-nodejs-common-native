'use strict';

var Q = require('q'),
    deepMerge = require('spacebox-common/src/deepMerge')

module.exports = {
    apply: function(ws, uuid, patch) {
        if (patch.tombstone === true) {
            delete ws[uuid]
        } else {
            var old = ws[uuid]
            if (old === undefined) {
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
                return

            var obj = JSON.parse(data)
            deepMerge(obj, storage)
        })
    },
}
