const claimsParser = require('../parsers/claims')

const uuid = require('uuid')

const buildClaims = async (context) => {
    let serialized = {}

    if (context.id) {
        serialized.id = context.id
    }

    if (context.session.id) {
        serialized.sessionId = context.session.id
    }

    if (context.role) {
        serialized.roleId = context.role.id
    }

    if (context.user) {
        serialized.userId = context.user.id
    }

    if (context.employee) {
        serialized.employeeId = context.employee.id
    }

    if (context.tenant) {
        serialized.tenantId = context.tenant.id
    }

    if (context.organization) {
        serialized.organizationId = context.organization.id
    }

    return serialized
}

const getContext = async (req, log, builder) => {
    let claims = claimsParser.parse(req, log)

    const isUser = (claims.role && claims.role.id) || (claims.session && claims.session.id)

    const context = builder ? await builder(claims, log) : claims

    context.id = context.id || claims.id || uuid.v1()

    context.config = context.config || {
        timeZone: 'IST'
    }

    let role = context.role

    if (!role && context.user && context.user.role) {
        role = context.user.role
    }

    context.permissions = context.permissions || []

    if (isUser) {
        context.permissions.push('user')
    } else {
        context.permissions.push('guest')
    }

    if (isUser) {
        if (context.organization) {
            context.permissions.push('organization.user')
        }

        if (context.tenant) {
            context.permissions.push('tenant.user')
        }
    } else {
        if (context.organization) {
            context.permissions.push('organization.guest')
        }

        if (context.tenant) {
            context.permissions.push('tenant.guest')
        }
    }

    context.logger = context.logger || log
    context.logger.context = context.logger.context || {}
    context.logger.context.id = context.id

    if (context.organization) {
        context.logger.context.organization = {
            id: context.organization.id,
            code: context.organization.code
        }
    }

    if (context.tenant) {
        context.logger.context.tenant = {
            id: context.tenant.id,
            code: context.tenant.code
        }
    }

    if (context.role) {
        context.logger.context.role = {
            id: context.role.id,
            code: context.role.code
        }
    }

    if (!context.hasPermission) {
        context.hasPermission = (request) => {
            if (!request) {
                return true
            }

            let items = Array.isArray(request) ? request : [request]

            return context.permissions.find(permission => {
                return items.find(item => item.toLowerCase() === permission)
            })
        }
    }

    if (!context.getConfig) {
        context.getConfig = (identifier, defaultValue) => {
            var keys = identifier.split('.')
            var value = context.config

            for (var key of keys) {
                if (!value[key]) {
                    return defaultValue
                }
                value = value[key]
            }

            return value
        }
    }

    return context
}

exports.serialize = async (entityName, action, entity, options, context) => {
    let serializedContext = options.context.serializer ? await options.context.serializer(context) : buildClaims(context)

    let serializedModel = entity
    if (options.models && options.models[entityName] && options.models[entityName].serializer) {
        serializedModel = await options.models[entityName].serializer(entity)
    }

    return JSON.stringify({
        context: serializedContext,
        entity: entityName,
        action: action,
        data: serializedModel
    })

}

exports.deserialize = async (message, options, logger) => {
    var data = JSON.parse(message)

    const context = await getContext(data.context, logger, options.context.deserializer)

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
