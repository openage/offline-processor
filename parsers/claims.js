
exports.parse = (serialized, logger) => {
    let claims = {
        id: serialized.id
    }

    if (serialized.sessionId) {
        claims.session = { id: serialized.sessionId }
    }

    if (serialized.roleId) {
        claims.role = { id: serialized.roleId }
    }

    if (serialized.userId) {
        claims.user = { id: serialized.userId }
    }

    if (serialized.employeeId) {
        claims.employee = { id: serialized.employeeId }
    }

    if (serialized.studentId) {
        claims.student = { id: serialized.studentId }
    }

    if (serialized.employeeId) {
        claims.employee = { id: serialized.employeeId }
    }

    if (serialized.tenantId) {
        claims.tenant = { id: serialized.tenantId }
    }    
    
    if (serialized.meta) {
        claims.meta = serialized.meta
    }

    logger.silly(claims)

    claims.logger = logger

    return claims
}
