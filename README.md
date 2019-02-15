# Offline Processor
A convention based pub/sub implementation over redis queue

Here are key features
- decouples the subscribers into separate files
- finds out relevant subscribers at run time
- plug-able subscribers, multiple subscribers for single action
- does nothing if subscribers does not exist
- convention based subscriber file locations
- passes around context from queuing to subscriber,
- configuration based serialization and de-serialization of context 
- configure as per load
- caches handler files

## Installation

```
npm install @open-age/offline-processor --save
```

## Usage

### Queue for offline processing
let say a `user` has `registered` and we need to `notify-admin`

```js
const queue = require("@offline-processor"); // adds [processor] context to the message that gets logged
const logger = require('@open-age/logger')('LISTEN')

const context = { // additional context information to be passed to processor
    permission: ['admin'],
    logger: logger
}

// registration code goes here ....

// lets handle rest of processing 

await queue.publish(
    'user', // entity type
    'registered', // action
    user, // the entity to be passed to processor
    context 
);

```

Based on the configuration 2 cases may result
1. The message is processed by looking finding the subscriber at `user/registered.js`
2. The message is published to the redis queue and the listener processes it using `user/registered.js`


### processing the message

The processor decouples the subscription into a separate file. By default the listener would look into the folder `processors` for file `${entityType}/${action}.js` (param cased/kebab case). 

Here is how the implementation would look like

```js
exports.subscribe = async (user, context) => {
    let logger = context.logger.start('fixing')
    emailClient.send(user.email, {
        subject: 'Welcome',
        body: `Congratulations, ....`
    })
    logger.end()
}
```

If you need to have multiple subscribers, you can have `defaults` folder inside the `user/registered`. Each file inside that folder would be treated as subscriber file.

NOTE: you can also implement `process` method instead of `subscribe`, but the support for `process` would be removed in next major release

## Configuration
Between publishing and processing we need to ensure that the offline processor is booted

### Initializing
Both the listener and the publisher process needs to initialize the offline-processor

```js

const logger = require('@open-age/logger')('booting')
const queue = require("@offline-processor")

let config = {
    ns: 'offline',
    queues: { 
        'default': 'offline'
    }
}
config.context = {
    serializer:  async (context)=>{
        return JSON.stringify(context)
    },
    deserializer: async (serialized, logger) => {
        return JSON.parse(serialized)
    }
}
config.models = {
    user: {
        get: async (id, context) {
            return await db.user.findById(id)
        }
    }
}

queue.initialize(config, logger)

```

### Start listening for the messages
You need to implement listener, so that you can start subscribing to the messages

```js

const logger = require('@open-age/logger')('LISTEN')
const queue = require("@offline-processor")

// listen for default queue
queue.listen(null, logger)

// listen for specified queues 
queue.listen(['offline', 'heavyLoad'], logger)

// listen for message on heavyLoad queue only
queue.listen(`heavyLoad`, logger)

// specify queue name as env variable
queue.listen(process.env.QUEUE_NAME, logger)

```

### additional configurations

1. Disable the offline mechanism for debugging purpose or in dev environment
```js 
    disabled = true
```

2. Custom redis server config 
```js 
{
    port:  6379,
    host:  '127.0.0.1',
    timeout: 30 * 60 * 1000, // 30 min
}
```

3. Custom location of processors files
```js
    processors: {
        dir: 'subscribers',
        default: {
            dir: 'defaults',
            file: 'default.js'
        }
    }
```

4. Separate queue for a particular `entity:action`
```js
    queues: { 
        'default': 'offline', // the default queue used for all the messages
        'file-import:time-log': 'heavyLoad'
    }
```
If no queue is specified, the queuing mechanism is disabled
