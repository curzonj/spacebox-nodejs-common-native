'use strict';

var ConnectionWrapper = require('./lib/pg-wrapper'),
    C = require("spacebox-common"),
    Q = require("q")

var logger

var self = module.exports = {
    db_select: function(name) {
        var database_url =
            process.env.DATABASE_URL ||
            process.env[name.toUpperCase()+'_DATABASE_URL']

        self.db = new ConnectionWrapper.build(database_url)
    },
    buildRedis: function() {
        var redisLib = require('promise-redis')(Q.Promise)

        var rtg   = require("url").parse(process.env.REDIS_URL)
        var redis = redisLib.createClient(rtg.port, rtg.hostname)

        if (logger === undefined)
            logger = C.logging.create()

        var port
        redis.on("error", function (err) {
            logger.info({ err: err, scope: 'redis' }, "connection error")
        });

        redis.on("end", function () {
            logger.info({ scope: 'redis', port: port }, "connection closed");
        });

        redis.on("ready", function () {
            port = redis.stream.localPort
            logger.info({ scope: 'redis', port: port }, "connection opened");
        });

        if (rtg.auth !== null)
            redis.auth(rtg.auth.split(":")[1]);

        return redis
    }
}
