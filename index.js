'use strict'
var queueConfig = require('config').get('queueServer')
var appRoot = require('app-root-path')
var fs = require('fs')
const changeCase = require('change-case')
let redisSMQ = require('rsmq')
var RSMQWorker = require('rsmq-worker')

const messageHelper = require('./helpers/message')

let redisQueue = null

let options = {
    disabled: false,
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
    queues: {},
    models: {},
    context: {
        serializer: (ctx) => Promise.cast(ctx),
        deserializer: (ctx) => Promise.cast(ctx),
        subscribers: (ctx) => Promise.cast([])
    }
}

let handlerFiles = {}
const setOptions = (config) => {
    options.disabled = config.disabled

    if (config.queues) {
        options.queues = config.queues
    }

    if (config.name) {
        options.queues.default = config.name
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

    if (config.options) {
        options.options = config.options
    }

    if (config.timeout) {
        options.timeout = config.timeout
    }
    
    if(config.password) {
        options.options = {password : config.password}
    }

    config.processors = config.processors || config.subscribers
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

setOptions(JSON.parse(JSON.stringify(queueConfig)) || {})

const handleDefaultProcessor = async (handler, entity, context) => {
    if (!(handler.process || handler.subscribe)) {
        context.logger.error(`no 'subscribe' method`)
        return Promise.resolve()
    }
    return new Promise((resolve, reject) => {
        let isHandled = false
        let promise = (handler.process || handler.subscribe)(entity, context, err => {
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

// const handleContextProcessor = async (file, entity, config, context) => {
//     let handler = require(file)
//     if (!handler.process) {
//         context.logger.error(`no 'process' method in ${file}`)
//         return Promise.resolve()
//     }
//     return new Promise((resolve, reject) => {
//         let isHandled = false
//         let promise = handler.process(entity, config, context, err => {
//             if (isHandled) { return }
//             isHandled = true
//             if (err) {
//                 context.logger.error(err)
//             }
//             resolve()
//         })

//         if (promise) {
//             promise.then(() => {
//                 if (isHandled) { return }
//                 isHandled = true
//                 resolve()
//             }).catch(err => {
//                 if (isHandled) { return }
//                 isHandled = true
//                 context.logger.error(err)
//                 resolve()
//             })
//         }
//     })
// }

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
            location: `${file}:subscribe`
        })
        let handler = require(file)
        await handleDefaultProcessor(handler, data, context)
        context.logger.end()
        context.logger = rootLogger
    }
}

const process = async (message, logger) => {
    let data = await messageHelper.deserialize(message, options, logger)
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

    const queues = []

    for (const key of Object.keys(options.queues)) {
        let queueName = options.queues[key]

        if (!queues.find(i => i === queueName)) {
            queues.push(queueName)
        }
    }

    if (!options.disabled && queues.length) {
        redisQueue = new redisSMQ({
            host: options.host,
            port: options.port,
            ns: options.ns,
            options: options.options || {}
        })

        for (const queueName of queues) {
            redisQueue.createQueue({
                qname: queueName,
                maxsize: -1
            }, (err, resp) => {
                if (err && err.message === 'Queue exists') {
                    log.info(`queue:${queueName} ${err.message}`)
                }
                if (resp === 1) {
                    log.info(`queue:${queueName} created`)
                }
            })
        }
    }
}
/**
 *
 * @param {string} entityName
 * @param {string} action
 * @param {*} data
 * @param {*} context
 */
exports.queue = async (entityName, action, data, context) => {
    let log = context.logger.start('publish')
    let queueName = options.queues[`${entityName}:${action}`] || options.queues.default

    if (!queueName || options.disabled || global.processSync || context.processSync) {
        log.silly('immediately processing', {
            entity: entityName,
            action: action
        })

        return handleMessage(entityName, action, data, context)
    }

    log.debug(`sending message to queue:${queueName}`, {
        entity: entityName,
        action: action
    })

    const message = await messageHelper.serialize(entityName, action, data, options, context)

    return new Promise((resolve, reject) => {
        redisQueue.sendMessage({
            qname: queueName,
            message: message
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

exports.publish = exports.queue

exports.listen = function (queueNames, logger) {
    queueNames = queueNames || options.queues.default

    let queues = []

    if (queueNames) {
        if (!Array.isArray(queueNames)) {
            queues.push(queueNames)
        } else {
            queues = queueNames
        }
    }

    if (!queues.length && options.queues.default) {
        queues.push(options.queues.default)
    }

    for (const queueName of queues) {
        let worker = workerFactory(queueName, logger)
        worker.start()
    }
}

const workerFactory = (queueName, logger) => {
    logger.info(`listening for messages on queue:${queueName}`)
    var worker = new RSMQWorker(queueName, {
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
        let log = logger.start(`${queueName}:${id}`)
        process(message, log).then(() => {
            log.end()
            next()
        }).catch(err => {
            log.error(err)
            log.end()
            next(err)
        })
    })

    return worker
}
