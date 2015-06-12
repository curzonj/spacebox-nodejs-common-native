'use strict';

var ConnectionWrapper = require('./lib/pg-wrapper'),
    Q = require("q")

module.exports.db_select = function(name) {
    var database_url =
        process.env.DATABASE_URL ||
        process.env[name.toUpperCase()+'_DATABASE_URL']

    module.exports.db = new ConnectionWrapper.build(database_url)
}

module.exports.buildRedis = function() {
    var redisLib = require('promise-redis')(Q.Promise)

    var rtg   = require("url").parse(process.env.REDIS_URL)
    var redis = redisLib.createClient(rtg.port, rtg.hostname)

    if (rtg.auth !== null)
        redis.auth(rtg.auth.split(":")[1]);

    return redis
}


