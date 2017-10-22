# offline-processor
redis based offline processor


## installation

```
npm install @open-age/offline-processor --save
```

## usage

### queue for offline processing
let say a `user` has `registered` and we need to `notify-admin`
```javascript
var offline = require("@offline-processor"); // adds [processor] context to the message that gets logged

// registration code goes here ....
// lets handle rest of processing offline
offline.queue(
    'user', // entity type
    'registered', // action
    user, // the entity to be passed to processor
    {} // additional context information to be passed to processor
); 
```

this should result in message in redis queue.

### listen and process the message
```js
require("@offline-processor").listen()
```

this would call `processor` method in all the files in `config.processors.dir`\\`user`\\`registered`\\`config.processors.default.dir`

you can place `notify-admin.js` there

## configure 

set it as part of `require('@open-age\offline-processor').initialize(params)` or part of `queueServer` section in config

```js
{
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
}
```
