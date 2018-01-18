package net.quasardb.qdb;

import net.quasardb.qdb.jni.*;

class QdbExceptionFactory {

    public static void throwIfError(int err) {
        if (qdb_error.severity(err) == qdb_err_severity.info)
            return;
        QdbException exception = createException(err);
        throw exception;
    }

    static QdbException createException(int err) {
        switch (err) {
        case qdb_error.connection_refused:
            return new QdbConnectionRefusedException();

        case qdb_error.host_not_found:
            return new QdbHostNotFoundException();

        case qdb_error.reserved_alias:
            return new QdbReservedAliasException();

        case qdb_error.invalid_argument:
            return new QdbInvalidArgumentException();

        case qdb_error.out_of_bounds:
            return new QdbOutOfBoundsException();

        case qdb_error.alias_not_found:
            return new QdbAliasNotFoundException();

        case qdb_error.alias_already_exists:
            return new QdbAliasAlreadyExistsException();

        case qdb_error.incompatible_type:
            return new QdbIncompatibleTypeException();

        case qdb_error.operation_disabled:
            return new QdbOperationDisabledException();

        case qdb_error.overflow:
            return new QdbOverflowException();

        case qdb_error.underflow:
            return new QdbUnderflowException();

        case qdb_error.resource_locked:
            return new QdbResourceLockedException();

        case qdb_error.invalid_reply:
            return new QdbInvalidReplyException();
        }

        String message = qdb.error_message(err);

        switch (qdb_error.origin(err)) {
        case qdb_err_origin.connection:
            return new QdbConnectionException(message);

        case qdb_err_origin.input:
            return new QdbInputException(message);

        case qdb_err_origin.operation:
            return new QdbOperationException(message);

        case qdb_err_origin.system_local:
            return new QdbLocalSystemException(message);

        case qdb_err_origin.system_remote:
            return new QdbRemoteSystemException(message);

        case qdb_err_origin.protocol:
            return new QdbProtocolException(message);
        }

        return new QdbException(message);
    }
}
