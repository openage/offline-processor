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
    let log = logger.start('kafka: offline:initialize')
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
        log.error(`There were some issue while connecting to kafkaserver: ${err}`)
        log.end()
        throw err
    }
}

const queue = async (message, topic, logger) => {
    let log = logger.start('kafka: offline:queue')

    return new Promise(async (resolve, reject) => {
        const payloads = {
            topic,
            messages: [{ value: message }]
        }
        log.info(`topic : ${topic} and messages: ${message} `)
        try {
            await producer.send(payloads)
            log.end()
            resolve()
        } catch (err) {
            log.error(`There were some issue while producing to kafkaserver: ${err}`)
            log.end()
            return reject(err)
        }
    })
}

const subscribe = async ({ process, topic, logger }) => {
    topic = topic || options.queues.default
    let log = logger.start('kafka: offline:subscribe')
    log.info(`consumerGroupId : ${options.consumerGroupId} `)

    let consumer = messageBroker.consumer({ groupId: options.consumerGroupId })
    await consumer.subscribe({ topic })
    await consumer.connect()
    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            log.info(`consuming the message topic: ${topic}, partition: ${partition}, message: ${JSON.stringify(message)}`)

            message = message.value.toString()
            process(message, log).then(() => {
                log.info(`Message consumed successfully`)
                log.end()
            }).catch(err => {
                log.error(`There were some issue while consuming from kafkaserver: ${err}`)
                log.end()
            })
        }
    })
}


module.exports = {
    initialize,
    queue,
    subscribe
}
