'use strict';

var Q = require('q'),
    Context = require('spacebox-common/src/logging.js'),
    util = require('util'),
    pgpLib = require('pg-promise'),
    uuidGen = require('node-uuid')

Q.longStackSupport = true

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

function ConnectionWrapper(connection, ctx) {
    this.connection = connection
    this.ctx = ctx
}

ConnectionWrapper.prototype.assertTx = function() {
    if (this.ctx.tx_id === undefined)
        throw new Error("Query not in a transaction")
}

ConnectionWrapper.prototype.tracing = function(ctx, fn) {
    if (this.ctx.id !== undefined)
        throw new Error("This connection already has tracing")

    return fn(new ConnectionWrapper(this.connection, ctx))
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

        return self.ctx.log_with(function(ctx) {
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

var query_counter = 0
var forwardFns = [ 'one', 'oneOrNone', 'none', 'query', 'any', 'many' ]
forwardFns.forEach(function(name) {
    ConnectionWrapper.prototype[name]= function(qstring) {
        var self = this

        query_counter = query_counter + 1
        var uuid = query_counter,
            interval_id = setInterval(function() {
                self.ctx.debug('psql', "query_id="+uuid+" query is blocked, still running")
                console.log('psql', "query_id="+uuid+" query is blocked, still running")
        }, 1000)

        self.ctx.debug('psql', "query_id="+uuid+" "+qstring)

        return Q(this.connection[name].apply(this.connection, arguments)).fail(function(e) {
            throw new SqlError(qstring, e.toString())
        }).fin(function() {
            clearInterval(interval_id)
        })
    }
})

var self = {
    db_select: function(name) {
        var pgp = pgpLib(/*options*/)
        var database_url =
            process.env.DATABASE_URL ||
            process.env[name.toUpperCase()+'_DATABASE_URL']

        self.db = new ConnectionWrapper(pgp(database_url), new Context())
    }
}

module.exports = self

