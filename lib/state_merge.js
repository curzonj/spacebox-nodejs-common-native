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
        function iter(cursor) {
            return redis.sscan('alive', cursor).
            then(function(reply) {
                return Q.all([
                    Q.fcall(function() {
                        if (reply[0] > 0)
                            return iter(reply[0])
                    }),
                    Q.fcall(function() {
                        if (reply[1].length > 0)
                            return redis.mget(reply[1])
                            .then(function(data) {
                                console.log(data)
                                data.forEach(function(v) {
                                    var obj = JSON.parse(v)
                                    storage[obj.uuid] = obj
                                    console.log('loaded', obj)
                                })
                            })
                    })
                ])
            })
        }

        return iter(0)
    },
}
