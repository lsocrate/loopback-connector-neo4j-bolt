'use strict'

const debug = require('debug')('loopback:connector:neo4j')
const { Connector } = require('loopback-connector')
const neo4j = require('neo4j-driver').v1

class Neo4j extends Connector {
  constructor (settings, dataSource) {
    super(...arguments)

    this.debug = settings.debug || debug.enabled
    if (this.debug) debug('Settings: %j', settings)

    this.dataSource = dataSource
  }
  connect (cb) {
    if (cb) {
      if (this.session) {
        return process.nextTick(() => cb(null, this.session))
      } else if (this.dataSource.connecting) {
        return this.dataSource.once('connected', () => {
          process.nextTick(() => cb(null, this.session))
        })
      }
    }

    const { url, port, user, password } = this.settings
    const authToken = neo4j.auth.basic(user, password)
    this.driver = neo4j.driver(`bolt://${url}:${port}`, authToken)
    this.session = this.driver.session()
  }
  disconnect (cb) {
    this.driver.close()
    this.session = null
    process.nextTick(cb)
  }
  ping (cb) {}

  /**
   * METHODS
   */
  create (model, data, options, cb) {}
  buildNearFilter (query, near) {}
  all (model, filter, options, cb) {}
  destroyAll (model, where, options, cb) {}
  count (model, where, options, cb) {}
  save (model, data, options, cb) {}
  update (model, where, data, options, cb) {}
  destroy (model, id, options, cb) {}
  updateAttributes (model, id, data, options, cb) {}

  /**
   * OPTIONAL
   */
  updateOrCreate (model, data, options, cb) {}
  findOrCreate (model, filter, data, cb) {}

  /**
   * NEW METHODS
   */
  replaceOrCreate (model, data, options, cb) {}
  replaceById (model, id, data, options, cb) {}
}

exports.initialize = function (dataSource, callback) {
  const settings = dataSource.settings || {}

  if (!neo4j) return

  dataSource.connector = new Neo4j(settings, dataSource)
  if (callback) return callback()
}
