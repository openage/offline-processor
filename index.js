'use strict'
var queueConfig = require('config').get('queueServer')
var appRoot = require('app-root-path')
var fs = require('fs')
const changeCase = require('change-case')
let redisSMQ = require('rsmq')
var RSMQWorker = require('rsmq-worker')

let redisQueue = null

let options = {
    disabled: false,
    name: 'offline',
    port: 6379,
    host: '127.0.0.1',
    ns: 'offline',
    timeout: 30 * 60 * 1000, // 30 min
    processors: {
        dir: 'actionHandlers',
        default: {
            dir: 'defaults',
            file: 'default.js'
        }
    },
    models: {},
    context: {
        serializer: (ctx) => Promise.cast(ctx),
        deserializer: (ctx) => Promise.cast(ctx),
        processors: (ctx) => Promise.cast([])
    }
}

let handlerFiles = {}
const setOptions = (config) => {
    options.disabled = config.disabled
    if (config.name) {
        options.name = config.name
    }
    if (config.port) {
        options.port = config.port
    }

    if (config.host) {
        options.host = config.host
    }

    if (config.ns) {
        options.ns = config.ns
    }

    if (config.timeout) {
        options.timeout = config.timeout
    }

    if (config.processors) {
        if (config.processors.dir) {
            options.processors.dir = config.processors.dir
        }

        if (config.processors.default) {
            if (config.processors.default.dir) {
                options.processors.default.dir = config.processors.default.dir
            }

            if (config.processors.default.file) {
                options.processors.default.file = config.processors.default.file
            }
        }
    }
    if (config.context) {
        if (config.context.serializer) {
            options.context.serializer = config.context.serializer
        }

        if (config.context.deserializer) {
            options.context.deserializer = config.context.deserializer
        }

        if (config.context.processors) {
            options.context.processors = config.context.processors
        }
    }

    if (config.models) {
        options.models = config.models
    }
}

setOptions(queueConfig || {})

const handleDefaultProcessor = async (handler, entity, context) => {
    if (!handler.process) {
        context.logger.error(`no 'process' method`)
        return Promise.resolve()
    }
    return new Promise((resolve, reject) => {
        let isHandled = false
        let promise = handler.process(entity, context, err => {
            if (isHandled) { return }
            isHandled = true
            if (err) {
                context.logger.error(err)
            }
            resolve()
        })

        if (promise) {
            promise.then(() => {
                if (isHandled) { return }
                isHandled = true
                resolve()
            }).catch(err => {
                if (isHandled) { return }
                isHandled = true
                context.logger.error(err)
                resolve()
            })
        }
    })
}

const handleContextProcessor = async (file, entity, config, context) => {
    let handler = require(file)
    if (!handler.process) {
        context.logger.error(`no 'process' method in ${file}`)
        return Promise.resolve()
    }
    return new Promise((resolve, reject) => {
        let isHandled = false
        let promise = handler.process(entity, config, context, err => {
            if (isHandled) { return }
            isHandled = true
            if (err) {
                context.logger.error(err)
            }
            resolve()
        })

        if (promise) {
            promise.then(() => {
                if (isHandled) { return }
                isHandled = true
                resolve()
            }).catch(err => {
                if (isHandled) { return }
                isHandled = true
                context.logger.error(err)
                resolve()
            })
        }
    })
}

const getHandlerFiles = (entity, action, context) => {
    if (handlerFiles[entity] && handlerFiles[entity][action]) {
        return handlerFiles[entity][action]
    }

    context.logger.silly(`looking handler files for ${entity}.${action}`)
    handlerFiles[entity] = handlerFiles[entity] || {}
    handlerFiles[entity][action] = handlerFiles[entity][action] || []

    const actionRoot = `${options.processors.dir}/${changeCase.paramCase(entity)}/${changeCase.paramCase(action)}`
    const root = `${appRoot}/${actionRoot}`

    let file = `${root}.js`
    if (fs.existsSync(file)) {
        handlerFiles[entity][action].push(file)
    }
    if (!fs.existsSync(root)) {
        return handlerFiles[entity][action]
    }
    file = `${root}/${options.processors.default.file}`
    if (fs.existsSync(file)) {
        handlerFiles[entity][action].push(file)
    }

    let dir = `${root}/${options.processors.default.dir}`
    if (!fs.existsSync(dir)) {
        return handlerFiles[entity][action]
    }
    for (let file of fs.readdirSync(dir)) {
        if (file.search('.js') < 0) {
            context.logger.error(`${file} is not .js`)
            return
        }
        handlerFiles[entity][action].push(`${dir}/${file}`)
    }

    return handlerFiles[entity][action]
}

const handleMessage = async (entity, action, data, context) => {
    let rootLogger = context.logger
    context.trigger = {
        entity: entity,
        action: action
    }

    for (const file of getHandlerFiles(entity, action, context)) {
        context.logger = rootLogger.start({
            location: `${file}:process`
        })
        let handler = require(file)
        await handleDefaultProcessor(handler, data, context)
        context.logger.end()
        context.logger = rootLogger
    }

    if (!options.context.processors) {
        return
    }
    let processors = await options.context.processors(context)

    const actionRoot = `${options.processors.dir}/${changeCase.paramCase(entity)}/${changeCase.paramCase(action)}`
    const root = `${appRoot}/${actionRoot}`
    for (let processor of processors) {
        let file = `${root}/${processor.name}.js`
        if (fs.existsSync(file)) {
            await handleContextProcessor(file, data, processor.config, context)
        }
    }
}

const deserialize = async (message, logger) => {
    var data = JSON.parse(message)

    let context = options.context.deserializer
        ? await options.context.deserializer(data.context, logger)
        : data.context

    context.logger = logger

    let model = data.data

    if (options.models && options.models[data.entity] && options.models[data.entity].deserializer) {
        model = await options.models[data.entity].deserializer(data.data, context)
    }

    return {
        context: context,
        model: model,
        entity: data.entity,
        action: data.action
    }
}
const process = async (message, logger) => {
    let data = await deserialize(message, logger)
    let description = data.model && data.model.id
        ? `${data.entity}/${data.model.id}/${data.action}`
        : `${data.entity}/${data.action}`
    let log = logger.start(`PROCESS ${description}`)

    data.context.logger = log

    await handleMessage(data.entity, data.action, data.model, data.context)
    log.end()
}

/**
 *
 * @param {*} params
 */
exports.initialize = (params, logger) => {
    let log = logger.start('offline:initialize')
    setOptions(params || {})
    if (!options.disabled) {
        redisQueue = new redisSMQ({
            host: options.host,
            port: options.port,
            ns: options.ns
        })

        redisQueue.createQueue({
            qname: options.name,
            maxsize: -1
        }, (err, resp) => {
            if (err && err.message === 'Queue exists') {
                log.info(`offline ${err.message}`)
            }
            if (resp === 1) {
                log.info(`offline created`)
            }
        })
    }
}
/**
 *
 * @param {string} entity
 * @param {string} action
 * @param {*} data
 * @param {*} context
 */
exports.queue = async (entity, action, data, context) => {
    let log = context.logger.start('offline:queue')

    if (options.disabled || global.processSync || context.processSync) {
        log.silly('immediately processing', {
            entity: entity,
            action: action
        })

        return handleMessage(entity, action, data, context)
    }

    log.debug('queuing for offline processing', {
        entity: entity,
        action: action
    })

    let serializedContext = options.context.serializer ? await options.context.serializer(context) : context
    let serializedModel = data
    if (options.models && options.models[entity] && options.models[entity].serializer) {
        serializedModel = await options.models[entity].serializer(data)
    }

    return new Promise((resolve, reject) => {
        redisQueue.sendMessage({
            qname: options.name,
            message: JSON.stringify({
                context: serializedContext,
                entity: entity,
                action: action,
                data: serializedModel
            })
        }, function (err, messageId) {
            if (err) {
                log.error(err)

                return reject(err)
            }
            if (messageId) {
                log.silly(`message queued id: ${messageId}`)
            }
            resolve()
        })
    })
}

exports.listen = function (logger) {
    logger.info('listening for messages')
    var worker = new RSMQWorker(options.name, {
        rsmq: redisQueue,
        timeout: options.timeout
    })

    worker.on('error', function (err, msg) {
        logger.error('error', {
            error: err,
            message: msg
        })
    })

    worker.on('exceeded', function (msg) {
        logger.error('exceeded', msg)
    })

    worker.on('timeout', function (msg) {
        logger.error('timeout', msg)
    })

    worker.on('message', function (message, next, id) {
        let log = logger.start(`${id}`)
        process(message, log).then(() => {
            log.end()
            next()
        }).catch(err => {
            log.error(err)
            log.end()
            next(err)
        })
    })

    worker.start()
}
