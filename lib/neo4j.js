'use strict'

const debug = require('debug')('loopback:connector:neo4j')
const { Connector } = require('loopback-connector')
const _ = require('lodash')
const neo4j = require('neo4j-driver').v1
const moment = require('moment')
const uuid = require('uuid')

function datesToUnix (input = {}) {
  return Object.entries(input).reduce(
    (data, [prop, val]) => {
      data[prop] = (val instanceof Date) ? moment(val).utc().valueOf() : val
      return data
    },
    {}
  )
}

class Neo4j extends Connector {
  constructor (settings, dataSource) {
    super(...arguments)

    this.settings = settings
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

    const { host, port, user, password, encrypted } = this.settings
    const authToken = neo4j.auth.basic(user, password)
    this.driver = neo4j.driver(`bolt://${host}:${port}`, authToken, { encrypted })
    this.session = this.driver.session()
    return process.nextTick(() => cb(null, this.session))
  }
  disconnect (cb) {
    this.driver.close()
    this.session = null
    process.nextTick(cb)
  }
  ping (cb) {
    this.session.run('RETURN 1')
      .then(() => cb(null, true))
      .catch(cb)
  }
  createUuid () { return uuid() }

  executeAsync (statement, parameters) { return this.session.run(...arguments) }
  execute (statement, parameters, cb) {
    this.executeAsync(statement, parameters)
      .then(res => cb(null, res))
      .catch(cb)
  }

  /**
   * METHODS
   */
  create (model, data, options, cb) {
    model = _.upperFirst(model)
    data = datesToUnix(data)
    if (_.isUndefined(data.id)) {
      data.id = this.createUuid()
    }

    const params = paramBlock(data)
    this.executeAsync(`CREATE (n:${model} ${params}) RETURN n`, data)
      .then(() => cb(null, data.id))
      .catch(cb)
  }
  buildNearFilter (query, near) {}
  all (model, filter, options, cb) {
    model = _.upperFirst(model)
    const pattern = paramBlock(filter.where)
    this.executeAsync(`MATCH (n:${model} ${pattern}) RETURN n`, filter.where)
      .then(({ records }) => cb(null, records.map(extractNode)))
      .catch(cb)
  }
  destroyAll (model, where, options, cb) {
    const id = where.id
    this.executeAsync('MATCH (n {id: {id}}) DELETE n RETURN n', { id })
      .then(() => cb(null))
      .catch(cb)
  }
  count (model, where, options, cb) {}
  save (model, data, options, cb) {
    console.log(2)
  }
  update (model, where, data, options, cb) {
    console.log(1)
  }
  destroy (model, id, options, cb) {
    console.log(model, id, options)
    cb()
  }
  updateAttributes (model, id, data, options, cb) {
    console.log(3)
  }
  replaceById (model, id, data, options, cb) {
    const params = { id, data }
    this.executeAsync('MATCH (n {id: {id}}) SET n={data} RETURN n', params)
      .then(({ records }) => cb(null, extractNode(records[0])))
      .catch(cb)
  }
}

function paramBlock (params) {
  const paramNames = Object.keys(params)
  if (!paramNames.length) return ''

  const paramList = paramNames.map(p => `${p}: {${p}}`).join(',')
  return `{${paramList}}`
}
function extractNode (record) {
  const node = record.toObject().n.properties
  return node
}

exports.initialize = function (dataSource, callback) {
  if (!neo4j) return

  const settings = dataSource.settings
  dataSource.connector = new Neo4j(settings, dataSource)

  if (callback) return dataSource.connector.connect(callback)
}
