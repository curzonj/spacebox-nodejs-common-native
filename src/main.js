'use strict';

var ConnectionWrapper = require('./pg-wrapper'),
    C = require("spacebox-common"),
    Q = require("q")

Q.longStackSupport = true

var self = module.exports = {
    db_select: function(name, ctx) {
        var database_url =
            process.env.DATABASE_URL ||
            process.env[name.toUpperCase()+'_DATABASE_URL']

        return new ConnectionWrapper.build(database_url, ctx)
    },
    buildRedis: function(logger, options) {
        if (logger === undefined)
            throw new Error("missing parameters")

        var redisLib = require('promise-redis')(Q.Promise)

        if (!options)
            options = {}

        options.retry_max_delay = 5000 // TODO configurable
        options.return_buffers = true

        var rtg   = require("url").parse(process.env.REDIS_URL)
        var redis = redisLib.createClient(rtg.port, rtg.hostname, options)

        var port
        redis.on("error", function (err) {
            logger.error({ err: err, scope: 'redis', port: port }, "connection error")
        });

        redis.on("end", function () {
            logger.warn({ scope: 'redis', port: port }, "connection closed");
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
