const { Kafka } = require('kafkajs')

let producer
var options = {
    queues: {},
    consumerGroupId: ""

}
let messageBroker

const setOptions = (config) => {
    options.disabled = config.disabled
    if (config.queues) {
        options.queues = config.queues
    }
    if (config.consumerGroupId) {
        options.consumerGroupId = config.consumerGroupId

    }
}

const initialize = async (params, logger) => {
    let log = logger.start('kafka:initialize')
    setOptions(params || {})
    let { brokers, clientId } = params
    log.info(`brokers : ${brokers} and clientId: ${clientId} isProducer: ${params.producer}`)
    try {
        messageBroker = new Kafka({
            clientId: clientId,
            brokers: brokers.split(","),
        })
        if (params.producer) {
            producer = messageBroker.producer()
            await producer.connect()
        }
        log.end()

    } catch (err) {
        log.error(err)
        log.end()
        throw err
    }
}

const queue = async (message, queue, logger) => {
    let log = logger.start('kafka:queue')
    const payload = {
        topic: queue,
        messages: [{ value: message }]
    }

    return new Promise(async (resolve, reject) => {
        try {
            await producer.send(payload)
            log.silly(`message queued`)
            log.end()
            resolve()
        } catch (err) {
            log.error(err)
            log.end()
            return reject(err)
        }
    })
}

const subscribe = async ({ process, topic, logger }) => {
    let log = logger.start(`kafka:subscribe:${topic}`)
    let groupId = options.consumerGroupId // TODO: should there be multiple group id - one for each topic?

    let consumer = messageBroker.consumer({ groupId: options.consumerGroupId })
    await consumer.subscribe({ topic })
    await consumer.connect()
    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {

            let consumerLog = logger.start(`${topic}:${message.id}`) // TODO: do we get any id of message
            consumerLog.silly(`topic: ${topic}, partition: ${partition}, message: ${JSON.stringify(message)}`)

            message = message.value.toString()
            process(message, log).then((id) => {
                consumerLog.info(`processed: ${id}`)
                consumerLog.end()
            }).catch(err => {
                consumerLog.error(err)
                consumerLog.end()
            })
        }
    })

    log.info('listening')
    log.end()
}

module.exports = {
    initialize,
    queue,
    subscribe
}
