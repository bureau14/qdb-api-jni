package net.quasardb.qdb.exception;

import net.quasardb.qdb.jni.*;

public class ExceptionFactory {

    public static void throwIfError(int err) {
        if (qdb_error.severity(err) == qdb_err_severity.info)
            return;
        Exception exception = createException(err);
        throw exception;
    }

    static Exception createException(int err) {
        switch (err) {
        case qdb_error.connection_refused:
            return new ConnectionRefusedException();

        case qdb_error.host_not_found:
            return new HostNotFoundException();

        case qdb_error.reserved_alias:
            return new ReservedAliasException();

        case qdb_error.invalid_argument:
            return new InvalidArgumentException();

        case qdb_error.out_of_bounds:
            return new OutOfBoundsException();

        case qdb_error.alias_not_found:
            return new AliasNotFoundException();

        case qdb_error.alias_already_exists:
            return new AliasAlreadyExistsException();

        case qdb_error.incompatible_type:
            return new IncompatibleTypeException();

        case qdb_error.operation_disabled:
            return new OperationDisabledException();

        case qdb_error.overflow:
            return new OverflowException();

        case qdb_error.underflow:
            return new UnderflowException();

        case qdb_error.resource_locked:
            return new ResourceLockedException();

        case qdb_error.invalid_reply:
            return new InvalidReplyException();
        }

        String message = qdb.error_message(err);

        switch (qdb_error.origin(err)) {
        case qdb_err_origin.connection:
            return new ConnectionException(message);

        case qdb_err_origin.input:
            return new InputException(message);

        case qdb_err_origin.operation:
            return new OperationException(message);

        case qdb_err_origin.system_local:
            return new LocalSystemException(message);

        case qdb_err_origin.system_remote:
            return new RemoteSystemException(message);

        case qdb_err_origin.protocol:
            return new ProtocolException(message);
        }

        return new Exception(message);
    }
}
