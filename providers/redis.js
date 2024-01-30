let redisSMQ = require('rsmq')
const RSMQWorker = require('rsmq-worker')
const queueConfig = require('config').get('queueServer')

var options = {
    disabled: false,
    port: 6379,
    host: '127.0.0.1',
    ns: 'offline',
    timeout: 30 * 60 * 1000, // 30 min
}
let messageBroker = null

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

setOptions(JSON.parse(JSON.stringify(queueConfig)) || {})

const initialize = async (params, logger) => {
    let log = logger.start('redis:initialize')
    setOptions(params || {})

    const queues = []

    for (const key of Object.keys(options.queues)) {
        let queueName = options.queues[key]

        if (!queues.find(i => i === queueName)) {
            queues.push(queueName)
        }
    }

    if (!options.disabled && queues.length) {
        messageBroker = new redisSMQ({
            host: options.host,
            port: options.port,
            ns: options.ns,
            options: options.options || {}
        })

        for (const queueName of queues) {
            messageBroker.createQueue({
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

    log.end()
}

const queue = async (message, queue, logger) => {
    let log = logger.start('redis:queue')

    const payload = {
        qname: queue,
        message: message
    }

    return new Promise((resolve, reject) => {
        messageBroker.sendMessage(payload, function (err, messageId) {
            if (err) {
                log.error(err)
                log.end()
                return reject(err)
            }
            log.silly(`message queued id: ${messageId}`)
            log.end()
            resolve()
        })
    })
}

const subscribe = ({ process, queueName, logger }) => {
    let log = logger.start(`redis:subscribe:${queueName}`)
    const consumer = new RSMQWorker(queueName, {
        rsmq: messageBroker,
        timeout: options.timeout
    })

    consumer.on('error', function (err, msg) {
        logger.error('error', {
            error: err,
            message: msg
        })
    })

    consumer.on('exceeded', function (msg) {
        logger.error('exceeded', msg)
    })

    consumer.on('timeout', function (msg) {
        logger.error('timeout', msg)
    })

    consumer.on('message', function (message, next, id) {
        let consumerLog = logger.start(`${queueName}:${id}`)
        process(message, log).then(() => {
            consumerLog.end()
            next()
        }).catch(err => {
            consumerLog.error(err)
            consumerLog.end()
            next(err)
        })
    })

    consumer.start()
    log.info('listening')
    log.end()
}


module.exports = {
    initialize,
    queue,
    subscribe
}
