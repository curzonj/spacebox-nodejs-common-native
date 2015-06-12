///'use strict';

var Q = require('q'),
    logging = require('spacebox-common/src/logging.js'),
    util = require('util'),
    pg = require('pg'),
    pgpLib = require('pg-promise'),
    uuidGen = require('node-uuid'),
    custom_pool = require('./pg-pool')

pg.defaults.poolLog = false
pg.defaults.poolIdleTimeout = 300000

Q.longStackSupport = true

var this_cn, pool
var pgp = pgpLib({
    promiseLib: Q,
    pgConnect: function(cn, cb) {
        if (cn !== this_cn) {
            console.log("wrong cn: "+cn)
            process.exit()
        }

        pool.connect(cb)
    }
})

function SqlError(query, sqlError) {
    Error.captureStackTrace(this, this.constructor)

    this.query = query
    this.sqlError = sqlError

    this.name = this.constructor.name
    this.message = [
        '`', query, '`',
        ' resulted in ',
        '`', sqlError, '`'
    ].join('')
}
util.inherits(SqlError, Error)

var extensions = []
function ConnectionWrapper(connection, ctx) {
    this.connection = connection
    this.ctx = ctx

    extensions.forEach(function(ext) {
        var stuff = ext(this)
        for (var k in stuff) {
            this[k] = stuff[k]
        }
    }, this)
}

ConnectionWrapper.extend = function(obj) {
    extensions.push(obj)
}

ConnectionWrapper.build = function(cn) {
    var ctx =  logging.create('psql')
    ctx.db_default = true

    this_cn = cn
    pool = ConnectionWrapper.pool = custom_pool.getOrCreate(this_cn)

    /*
    setInterval(function() {
        console.log({
            waitingClients: pool.waitingClientsCount(),
            availableObjectsCount: pool.availableObjectsCount()
        })
    }, 1000)
    */

    return new ConnectionWrapper(pgp(cn), ctx)
}

ConnectionWrapper.prototype.as = pgp.as

ConnectionWrapper.prototype.assertTx = function() {
    if (this.ctx.tx_id === undefined)
        throw new Error("Query not in a transaction")
}

ConnectionWrapper.prototype.tracing = function(ctx, fn) {
/* This is another option, but you might start getting
 * duplicate context elements if you instrument twice
 * with the same context. If you double instrument the
 * db, it's probably a bug anyways.
 *
    var new_ctx = new Context(ctx)
    new_ctx.extend(this.ctx)

    var conn = new ConnectionWrapper(this.connection, new_ctx)
*/
    if (this.ctx.db_default !== true)
        throw new Error("This connection already has tracing")

    var conn = new ConnectionWrapper(this.connection, ctx)

    if (fn !== undefined) {
      return fn(conn)
    } else {
      return conn
    }
}

var tx_counter = 0
ConnectionWrapper.prototype.tx = function(fn) {
    var self = this

    // alternate signature: tx(ctx, function() {
    if (typeof fn !== 'function') {
        var original_args = arguments
        return self.tracing(original_args[0], function(traced_db) {
            return traced_db.tx(original_args[1])
        })
    }

    if (self.ctx.tx_id === undefined) {
        tx_counter = tx_counter + 1
        var uuid = tx_counter

        return self.ctx.old_log_with(function(ctx) {
            ctx.tx_id = uuid

            return Q(self.connection.tx(function(tx) {
                ctx.debug('psql', "transaction open")
                return fn(new ConnectionWrapper(tx, ctx))
            })).fin(function() {
                ctx.debug('psql', "transaction closed")
            })
        }, [ 'tx='+uuid ], 'psql')
    } else {
        // Don't second guess the actual library, it'll won't open
        // a new transaction if it doesn't need to, but don't generate
        // a new logging context if we don't think we need one
        return Q(self.connection.tx(function(tx) {
            return fn(new ConnectionWrapper(tx, self.ctx))
        }))
    }
}

function queryWrapper(qstring, values, wrapper) {
    var scopedError = new Error("hung query: "+qstring)

    var uuid = uuidGen.v1(),
        interval_id = setInterval(function() {
            wrapper.ctx.warn({
                query_id: uuid,
                waitingClients: pool.waitingClientsCount(),
                availableObjectsCount: pool.availableObjectsCount()
            }, " query is blocked, still running")
        }, 1000)

    wrapper.ctx.debug({ query_id: uuid, sql: qstring, values: values }, 'sql query')

    return interval_id
}

ConnectionWrapper.prototype.prepared = function(obj) {
    var self =this,
        deferred = Q.defer()

    var fn, interval_id = queryWrapper(obj.text, obj.values, self)

    if (typeof this.connection.getClient === 'function') {
        fn = function(cb) {
            //console.log('reusing transaction connection')
            cb(self.connection.getClient(), function() { /* noop */ })
        }
    } else if(self.ctx.tx_id !== undefined) {
        throw new Error("failed to find the connection for this transaction")
    } else {
        fn = function(cb) {
            pool.connect(function(err, client, done) {
                if (err)
                    return done(err)

                cb(client, done)
            })
        }
    }

    fn(function(client, done) {
        client.query(obj, function(err, result) {
            done(err)
            clearInterval(interval_id)

            if(err) {
                deferred.reject(err)
            } else {
                deferred.resolve(result)
            
            }
        })
    })

    return deferred.promise
}

var forwardFns = [ 'one', 'oneOrNone', 'none', 'query', 'any', 'many' ]
forwardFns.forEach(function(name) {
    ConnectionWrapper.prototype[name]= function(qstring, values) {
        var interval_id = queryWrapper(qstring, values, this)

        return Q(this.connection[name].apply(this.connection, arguments)).fail(function(e) {
            throw new SqlError(qstring, e.toString())
        }).fin(function() {
            clearInterval(interval_id)
        })
    }
})

module.exports = ConnectionWrapper
