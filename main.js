'use strict';

var ConnectionWrapper = require('./lib/pg-wrapper'),
    C = require("spacebox-common"),
    Q = require("q")

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

        if (rtg.auth !== null)
            redis.auth(rtg.auth.split(":")[1]);

        return redis
    },
    defineSystemMetrics: function() {
        C.stats.defineAll({
            memoryRss: 'histogram',
            memoryHeapTotal: 'histogram',
            memoryHeapUsed: 'histogram',
            schedulingJitter: 'histogram',
        })

        var lastRun
        setInterval(function() {
            if (lastRun === undefined) {
                lastRun = Date.now()
            } else {
                var thisRun = Date.now()
                C.stats.schedulingJitter.update(lastRun + 500 - thisRun)
                lastRun = thisRun
            }

            var usage = process.memoryUsage()
            C.stats.memoryRss.update(usage.rss)
            C.stats.memoryHeapTotal.update(usage.heapTotal)
            C.stats.memoryHeapUsed.update(usage.heapUsed)
        }, 500)
    }
}

self.defineSystemMetrics()
