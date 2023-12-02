let redisSMQ = require('rsmq')
var RSMQWorker = require('rsmq-worker')
var msgbrokerConfig = require('config').get('queueServer')

var options = {
    disabled: false,
    port: 6379,
    host: '127.0.0.1',
    ns: 'offline',
    timeout: 30 * 60 * 1000, // 30 min
}
let redisQueue = null

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

    if (config.password) {
        options.options = { password: config.password }
    }
}

setOptions(JSON.parse(JSON.stringify(msgbrokerConfig)) || {})

const initialize = async (params, logger) => {
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
const queue = async (message, queue, log) => {
    return new Promise((resolve, reject) => {
        redisQueue.sendMessage({
            qname: queue,
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

const subscribe = ({ process, queueNames, logger }) => {
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
        let worker = workerFactory(process, queueName, logger)
        worker.start()
    }
}

const workerFactory = (process, queueName, logger) => {
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


module.exports = {
    initialize,
    queue,
    subscribe
}
