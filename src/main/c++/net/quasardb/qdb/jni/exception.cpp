#include "exception.h"
#include "env.h"
#include "introspect.h"
#include "string.h"

std::string
_error_code_to_exception_class_name(qdb_error_t e)
{

    switch (e)
    {
    case qdb_e_connection_refused:
        return "net/quasardb/qdb/exception/ConnectionRefusedException";

    case qdb_e_host_not_found:
        return "net/quasardb/qdb/exception/HostNotFoundException";

    case qdb_e_reserved_alias:
        return "net/quasardb/qdb/exception/ReservedAliasException";

    case qdb_e_invalid_argument:
        return "net/quasardb/qdb/exception/InvalidArgumentException";

    case qdb_e_out_of_bounds:
        return "net/quasardb/qdb/exception/OutOfBoundsException";

    case qdb_e_alias_not_found:
        return "net/quasardb/qdb/exception/AliasNotFoundException";

    case qdb_e_alias_already_exists:
        return "net/quasardb/qdb/exception/AliasAlreadyExistsException";

    case qdb_e_incompatible_type:
        return "net/quasardb/qdb/exception/IncompatibleTypeException";

    case qdb_e_operation_disabled:
        return "net/quasardb/qdb/exception/OperationDisabledException";

    case qdb_e_overflow:
        return "net/quasardb/qdb/exception/OverflowException";

    case qdb_e_underflow:
        return "net/quasardb/qdb/exception/UnderflowException";

    case qdb_e_resource_locked:
        return "net/quasardb/qdb/exception/ResourceLockedException";

    case qdb_e_invalid_reply:
        return "net/quasardb/qdb/exception/InvalidReplyException";

    case qdb_e_interrupted:
        return "net/quasardb/qdb/exception/InterruptedException";

    case qdb_e_network_inbuf_too_small:
        return "net/quasardb/qdb/exception/InputBufferTooSmallException";
    }

    switch (QDB_ERROR_ORIGIN(e))
    {
    case qdb_e_origin_connection:
        return "net/quasardb/qdb/exception/ConnectionException";

    case qdb_e_origin_input:
        return "net/quasardb/qdb/exception/InputException";

    case qdb_e_origin_operation:
        return "net/quasardb/qdb/exception/OperationException";

    case qdb_e_origin_system_local:
        return "net/quasardb/qdb/exception/LocalSystemException";

    case qdb_e_origin_system_remote:
        return "net/quasardb/qdb/exception/RemoteSystemException";
    }

    return "net/quasardb/qdb/exception/Exception";
}

void
qdb::jni::exception::throw_new(jni::env &env) const noexcept
{
    // Convert our error code to a pretty Java exception class
    jclass exception_class = introspect::lookup_class(
        env, _error_code_to_exception_class_name(_error).c_str());

    // And put it on the stack!
    env.instance().ThrowNew(exception_class, _msg.c_str());

    // Note that execution just continues as normal,
}

qdb_error_t
qdb::jni::exception::throw_if_error(qdb_handle_t h, qdb_error_t e)
{

    if (QDB_FAILURE(e))
    {

        qdb_string_t what;
        qdb_error_t err;
        qdb_get_last_error(h, &err, &what);

        if (err != e)
        {
            //! In some rare circumstances, QuasarDB loses the original error
            //! code.
            what.data = qdb_error(e);
        }

        throw qdb::jni::exception{e, what.data};
    }

    return e;
}
