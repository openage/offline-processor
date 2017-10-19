"use strict";
var logger = require('@open-age/logger')('offline-processor');
var config = require('config').get('queueServer');
var appRoot = require('app-root-path');
var _ = require('underscore');
var async = require('async');
var fs = require('fs');
var paramCase = require('param-case');
let redisSMQ = require('rsmq');
var RSMQWorker = require("rsmq-worker");

config.name = config.name || 'offline';
config.timeout = config.timeout || 30 * 60 * 1000; //default 3000 ~ now 30 min
config.processors = config.processors || {
    dir: 'actionHandlers'
};
config.processors.default = config.processors.default || {
    dir: 'defaults',
    file: 'default.js'
};

config.context = {
    serializer: null,
    deserializer: null,
    processors: null
};

let redisQueue = null;
if (!config.disabled) {
    redisQueue = new redisSMQ({
        host: config.host,
        port: config.port,
        ns: config.ns
    });

    redisQueue.createQueue({
        qname: config.name,
        maxsize: -1
    }, function (err, resp) {
        if (err && err.message === "Queue exists") {
            logger.info(`offline ${err.message}`);
        }
        if (resp === 1) {
            logger.info(`offline created`);
        }
    });
}

const initialize = function (params) {

    params = params || {};
    if (params.disabled === true) {
        config.disabled = true;
    }
    params.context = params.context || {};
    if (params.context.serializer) {
        config.context.serializer = params.context.serializer;
    }

    if (params.context.deserializer) {
        config.context.deserializer = params.context.deserializer;
    }

    if (params.context.processors) {
        config.context.processors = params.context.processors;
    }
};

const handleDefaultProcessors = (files, data, context, onDone) => {

    if (_.isEmpty(files)) {
        return onDone(null);
    }
    async.eachSeries(files, (file, cb) => {
        let handler = require(file);
        if (!handler.process) {
            return cb(null);
        }
        logger.debug('processing', {
            handler: file
        });
        handler.process(data, context, err => {
            if (err) {
                logger.error(err);
            }
            cb(err);
        });
    }, onDone);
};

const handleInsightProcessor = (file, data, config, context) => {
    let handler = require(file);
    if (!handler.process) {
        return Promise.cast(null);
    }
    logger.debug('processing', {
        handler: file
    });
    return handler.process(data, config, context);
};

const queueMessage = function (entity, action, data, context, callback) {

    redisQueue.sendMessage({
        qname: 'offline',
        message: JSON.stringify({
            context: context,
            entity: entity,
            action: action,
            data: data
        })
    }, function (err, messageId) {
        if (err) {
            logger.error(err);
        }
        if (messageId) {
            logger.debug(`message queued id: ${messageId}`);
        }
        if (callback) {
            callback(err, messageId);
        }
    });
};

const listen = function () {

    logger.info('listening for messages');
    var worker = new RSMQWorker(config.name, {
        rsmq: redisQueue,
        timeout: config.timeout
    });

    worker.on('error', function (err, msg) {
        logger.error('error', {
            error: err,
            message: msg
        });
    });

    worker.on('exceeded', function (msg) {
        logger.error('exceeded', msg);
    });

    worker.on('timeout', function (msg) {
        logger.error('timeout', msg);
    });

    worker.on("message", function (message, next, id) {

        if (id) {
            logger.debug(`processing message id: ${id}`);
        }

        process(message, next);
    });
};

const handleMessage = function (data, context, callback) {
    const root = `${appRoot}/${config.processors.dir}/${paramCase(context.entity)}/${paramCase(context.action)}`;
    if (!fs.existsSync(root)) {
        return callback();
    }
    async.waterfall([cb => {
        if (!config.context.deserializer) {
            return cb(null, context);
        }
        return config.context.deserializer(context).then(item => cb(null, item)).catch(err => cb(err));

    }, cb => { // default actions
        let handlerFiles = [];
        let file = `${root}/${config.processors.default.file}`;
        if (fs.existsSync(file)) {
            handlerFiles.push(file);
        }

        let dir = `${root}/${config.processors.default.dir}`;
        if (fs.existsSync(dir)) {
            _.each(fs.readdirSync(dir), function (file) {
                if (file.search('.js') < 0) {
                    logger.error(`${file} is not .js`);
                    return;
                }
                handlerFiles.push(`${dir}/${file}`);
            });
        }

        handleDefaultProcessors(handlerFiles, data, context, cb);
    }, cb => {
        if (config.context.processors) {
            return cb(null, []);
        }

        return config.context.processors(context).then(items => cb(null, items)).catch(err => cb(err));

    }, (processors, cb) => {

        Promise.all(processors.map(processor => {

            var fileName = `${root}/${processor.name}`;

            // if (file.process('.js') < 0) {
            //     logger.error(`${file} is not .js`);
            //     return Promise.cast(null);
            // }

            return handleInsightProcessor(fileName, data, processor.config, context);
        })).then(() => cb()).catch(err => cb(err));
    }], callback);
};

const process = (message, callback) => {
    var data = JSON.parse(message);
    if (!callback) {
        callback = (err) => {
            logger.error(err);
        };
    }
    data.context.entity = data.entity;
    data.context.action = data.action;
    return handleMessage(data.data, data.context, callback);
};

const queue = (entity, action, data, context) => {
    context.entity = entity;
    context.action = action;

    if (config.disabled || global.processSync || context.processSync) {
        logger.debug('immediately processing', {
            entity: entity,
            action: action
        });

        return new Promise((resolve, reject) => {
            handleMessage(data, context, (err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    logger.debug('queuing for offline processing', {
        entity: entity,
        action: action
    });

    if (config.context.serializer) {
        return config.context.serializer(context).then(context => {
            return new Promise((resolve, reject) => {
                queueMessage(entity, action, data, context, function (err) {
                    if (err) {
                        reject(err);
                    } else {
                        resolve();
                    }
                });
            });
        });
    }
};

exports.initialize = initialize;
exports.queue = queue;
exports.listen = listen;
