_ = require 'lodash'
Rabbit = require('amqplib')
Job = require('./job')

module.exports = class Queue

  constructor: (@name, options, @connection) ->
    @log = @connection.log
    @stats = @connection.stats
    @options = _.defaultsDeep options, {
      name: @name
      type: @connection.exchange.name + '.' + @name
      timeout: null
      concurrency: 1
      id: 0,
      durable: true,
      autoDelete: false
    }

    @connected = false
    @stack = @connection.queues?[@name]?.stack ? []
    @lastPublish = 0
    @lastComplete = 0

  connect: (cb) ->
    {name, type, concurrency} = @options
    
    @channel?.close?()
    
    return @connection.connection.createChannel()
      .then (@channel) =>
        @channel.assertQueue type, _.omit @options, ['name', 'type', 'concurrency']
      .then =>
        @channel.bindQueue(type, @connection.exchange.name, type)
      .then =>
        @channel.recover()
      .then =>
        @channel.prefetch(concurrency)
      .then =>
        @channel.consume type, @processJob.bind(@)
      .then =>
        # stats the number of messages and consumers
        timer = setInterval (=>
          @channel.checkQueue(type).then (ok) =>
            @stats 'increment', type, 'consumers', ok.consumerCount
            @stats 'increment', type, 'messages', ok.messageCount
        ), 30 * 1000

        # reconnect on close
        @channel.on 'close', =>
          @log.error {type}, 'Channel closed. Reconnecting.'
          clearTimeout(timer)
          @connected = false
          @connect()

        # Log on error
        @channel.on 'error', (err) =>
          @log.error {type}, 'Channel errored.', err

        # Log on returned/unroutable message
        @channel.on 'returned', (msg) =>
          @log.error {type}, 'Unroutable message returned!'

        # after connection, publish any held messages
        @connected = true
        setTimeout (=>
          for item in @stack
            @[item.type](item.body, item.options, item.cb)
        ), 100

        cb? null, @

      .catch (err) =>
        @log.error {type}, "Could not connect queue", err.stack
        setTimeout (=> @connect(cb)), 1000

  publish: (body, options, cb) ->
    unless @connected
      return @stack.push {type: 'publish', body, options, cb}

    job = new Job @options.type, @
    job.publish body, options, cb

  request: (body, options, cb) ->
    unless @connected
      return @stack.push {type: 'request', body, options, cb}

    job = new Job @options.type, @
    job.request body, options, cb

  processJob: (message) ->
    job = new Job @options.type, @
    job.process message
  
  partFailure: (message) ->
    @options.partFailure? message
    @connection.partFailure? message

  fullFailure: (message) ->
    @options.fullFailure? message
    @connection.fullFailure? message    