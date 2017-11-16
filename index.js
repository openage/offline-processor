"use strict";
var logger = require('@open-age/logger')('offline-processor');
var queueConfig = require('config').get('queueServer');
var appRoot = require('app-root-path');
var _ = require('underscore');
var async = require('async');
var fs = require('fs');
var paramCase = require('param-case');
let redisSMQ = require('rsmq');
var RSMQWorker = require("rsmq-worker");

let redisQueue = null;

let options = {
    disabled: false,
    name:  'offline',
    port:  6379,
    host:  '127.0.0.1',
    ns: 'offline',
    timeout: 30 * 60 * 1000, // 30 min
    processors: {
        dir: 'actionHandlers',
        default: {
            dir: 'defaults',
            file: 'default.js'
        }
    },
    context: {
        serializer: (ctx)=>Promise.cast(ctx),
        deserializer: (ctx)=>Promise.cast(ctx),
        processors: (ctx)=>Promise.cast([])
    }
};
const setOptions = (config) => {
    options.disabled = config.disabled;
    if (config.name) {
        options.name = config.name;
    }
    if (config.port) {
        options.port = config.port;
    }

    if (config.host) {
        options.host = config.host;
    }

    if (config.ns) {
        options.ns = config.ns;
    }

    if (config.timeout) {
        options.timeout = config.timeout;
    }

    if (config.processors) {
        if (config.processors.dir) {
            options.processors.dir = config.processors.dir;
        }

        if (config.processors.default) {
            if (config.processors.default.dir) {
                options.processors.default.dir = config.processors.default.dir;
            }

            if (config.processors.default.file) {
                options.processors.default.file = config.processors.default.file;
            }
        }
    }
    if (config.context) {
        if (config.context.serializer) {
            options.context.serializer = config.context.serializer;
        }

        if (config.context.deserializer) {
            options.context.deserializer = config.context.deserializer;
        }

        if (config.context.processors) {
            options.context.processors = config.context.processors;
        }
    }

    
};

setOptions(queueConfig);

/**
 * 
 * @param {*} params 
 */
const initialize = function (params) {

    setOptions(params);
    if (!options.disabled) {
        redisQueue = new redisSMQ({
            host: options.host,
            port: options.port,
            ns: options.ns
        });

        redisQueue.createQueue({
            qname: options.name,
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
    var worker = new RSMQWorker(options.name, {
        rsmq: redisQueue,
        timeout: options.timeout
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
    const root = `${appRoot}/${options.processors.dir}/${paramCase(context.entity)}/${paramCase(context.action)}`;
    if (!fs.existsSync(root)) {
        return callback();
    }
    async.waterfall([cb => {
        if (!options.context.deserializer) {
            return cb(null, context);
        }
        return options.context.deserializer(context).then(item => cb(null, item)).catch(err => cb(err));

    }, cb => { // default actions
        let handlerFiles = [];
        let file = `${root}/${options.processors.default.file}`;
        if (fs.existsSync(file)) {
            handlerFiles.push(file);
        }

        let dir = `${root}/${options.processors.default.dir}`;
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
        if (!options.context.processors) {
            return cb(null, []);
        }

        return options.context.processors(context).then(items => cb(null, items)).catch(err => cb(err));

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

/**
 * 
 * @param {string} entity 
 * @param {string} action 
 * @param {*} data 
 * @param {*} context 
 */
const queue = (entity, action, data, context) => {
    context.entity = entity;
    context.action = action;

    if (options.disabled || global.processSync || context.processSync) {
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

    if (options.context.serializer) {
        return options.context.serializer(context).then(context => {
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