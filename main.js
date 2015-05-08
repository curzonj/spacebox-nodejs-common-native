'use strict';

var Q = require('q'),
    util = require('util'),
    debug = require('debug'),
    pgpLib = require('pg-promise')

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

var self = {
    db: {
        select: function(name) {
            var pgp = pgpLib(/*options*/)
            var database_url =
                process.env.DATABASE_URL ||
                process.env[name.toUpperCase()+'_DATABASE_URL']

            self.db.connection = pgp(database_url)
        },
        query: function(qstring) {
            if (self.db.connection === undefined)
                throw new Error("Database has not been initialized yet")

            return Q(self.db.connection.query.apply(self.db.connection, arguments)).fail(function(e) {

                throw new SqlError(qstring, e.toString())
            })
        }
    }
}

module.exports = self

