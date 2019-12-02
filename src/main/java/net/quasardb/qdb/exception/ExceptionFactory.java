package net.quasardb.qdb.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.quasardb.qdb.jni.*;

public class ExceptionFactory {
    private static final Logger logger = LoggerFactory.getLogger(ExceptionFactory.class);

    public static void throwIfError(int err) {
        logger.trace("validating error code: ", err);
        if (qdb_error.severity(err) == qdb_err_severity.info)
            return;
        Exception exception = createException(err);
        throw exception;
    }

    public static Exception createException(int err) {
        logger.debug("creating exception for error with code: " + err + ", inbuf error code = " + qdb_error.network_inbuf_too_small);

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

        case qdb_error.interrupted:
            return new InterruptedException();

        case qdb_error.network_inbuf_too_small:
            return new InputBufferTooSmallException();

        };

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
        };

        return new Exception(message);
    }
}
