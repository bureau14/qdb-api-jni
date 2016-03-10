%module(package="qdb") qdb
#pragma SWIG nowarn=453

// we need to force the mapping of qdb_time_t to a long otherwise swig tries to create an intermediate object
%typemap(jni)       qdb_time_t "jlong"
%typemap(jtype)     qdb_time_t "long"
%typemap(jstype)    qdb_time_t "long"
%typemap(javain)    qdb_time_t "$javainput"
%typemap(in)        qdb_time_t %{ $1 = $input; %}
%typemap(out)       qdb_time_t  %{ $result = $1; %}
%typemap(javaout)   qdb_time_t { return $jnicall; }

%typemap(jni)       qdb_int_t "jlong"
%typemap(jtype)     qdb_int_t "long"
%typemap(jstype)    qdb_int_t "long"
%typemap(javain)    qdb_int_t "$javainput"
%typemap(in)        qdb_int_t %{ $1 = $input; %}
%typemap(out)       qdb_int_t  %{ $result = $1; %}
%typemap(javaout)   qdb_int_t { return $jnicall; }

%{

#include <qdb/client.hpp>

%}

%include "typemaps.i"
%include "std_string.i"
%include "std_shared_ptr.i"
%include "carrays.i"
%include "std_vector.i"

typedef struct qdb_session * qdb_handle_t;
%nodefaultdtor qdb_handle_t;

%template(ApiBufferPtr) std::shared_ptr<qdb::api_buffer>;

%rename("%(regex:/qdb_e(.*)/error\\1/)s", %$isenumitem) "";
%rename("%(regex:/qdb_comp(.*)/compression\\1/)s", %$isenumitem) "";
%rename("%(strip:[qdb_])s", %$isfunction) "";

// we need to do this otherwise SWIG will try to release the ByteBuffer as if it were a String
%typemap(memberin)  void * BUFFER "$1 = $input;"
%typemap(freearg, noblock=1)   void * BUFFER ""

%typemap(jni)       void * BUFFER "jobject"
%typemap(jtype)     void * BUFFER "java.nio.ByteBuffer"
%typemap(jstype)    void * BUFFER "java.nio.ByteBuffer"
%typemap(javain)    void * BUFFER "$javainput"
%typemap(javaout)   void * BUFFER { return $jnicall; }

%apply void * BUFFER { char * content };
// we have false warning because of size_t -> long
// and there is no unsigned type in Java
%apply void * BUFFER { char * comparand };
%apply void * BUFFER { char * result };

%apply unsigned long { size_t };
%apply unsigned long { qdb_time_t };

%typemap(in) char * content %{
    /* %typemap(in) char * content */
    if ($input)
    {
        arg1->content_size = static_cast<qdb_size_t>(jenv->GetDirectBufferCapacity($input));
        if (arg1->content_size == -1)
        {
            SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get the capacity of direct buffer. Buffer must be allocated direct.");
        }

        $1 = ($1_ltype)jenv->GetDirectBufferAddress($input);
        if (!$1)
        {
            SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get the address of direct buffer. Buffer must be allocated direct.");
        }
    }
    else
    {
        $1 = nullptr;
        arg1->content_size = 0;
    }
%}

%typemap(out) char * content %{
    $result = ($1 && arg1->content_size) ? jenv->NewDirectByteBuffer($1, arg1->content_size) : nullptr;
%}

%typemap(in) char * comparand %{
    /* %typemap(in) char * comparand */
    if ($input)
    {
        arg1->comparand_size = static_cast<qdb_size_t>(jenv->GetDirectBufferCapacity($input));
        if (arg1->content_size == -1)
        {
            SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get the capacity of direct buffer. Buffer must be allocated direct.");
        }

        $1 = ($1_ltype)jenv->GetDirectBufferAddress($input);
        if (!$1)
        {
            SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get the address of direct buffer. Buffer must be allocated direct.");
        }
    }
    else
    {
        $1 = nullptr;
        arg1->content_size = 0;
    }
%}

%typemap(out) char * comparand %{
    $result = ($1 && arg1->comparand_size) ? jenv->NewDirectByteBuffer($1, arg1->comparand_size) : nullptr;
%}

%typemap(out) char * result %{
    $result = ($1 && arg1->result_size) ? jenv->NewDirectByteBuffer($1, arg1->result_size) : nullptr;
%}

%include "qdb_enum.i"
%include "qdb_struct.i"

%rename(version) qdb_version;
const char * qdb_version();

%rename(build) qdb_build;
const char * qdb_build();

%rename(open) qdb_open_tcp;
qdb_handle_t qdb_open_tcp();

qdb_error_t qdb_close(qdb_handle_t handle);

qdb_error_t qdb_connect(qdb_handle_t handle, const char * uri);

qdb_error_t qdb_stop_node(
    qdb_handle_t handle,
    const char * uri,
    const char * reason);

%typemap(in) const char * content {
    /* %typemap(in) const char * content */
    $1 = ($1_ltype)jenv->GetDirectBufferAddress($input);
    if ($1 == 0)
    {
        SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get the address of direct buffer. Buffer must be allocated direct.");
    }
}

// we need to do this otherwise SWIG will try to release the ByteBuffer as if it were a String
%typemap(freearg, noblock=1) const char * content ""

%typemap(jni)     const char * content "jobject"
%typemap(jtype)   const char * content "java.nio.ByteBuffer"
%typemap(jstype)  const char * content "java.nio.ByteBuffer"
%typemap(javain)  const char * content "$javainput"
%typemap(javaout) const char * content { return $jnicall; }

%typemap(jni) retval  "jobject"
%typemap(jtype) retval  "java.nio.ByteBuffer"
%typemap(jstype) retval "java.nio.ByteBuffer"

%typemap(out) retval  %{
    if ($1.buffer && $1.buffer_size)
    {
        $result = jenv->NewDirectByteBuffer($1.buffer, $1.buffer_size);
    }
    else
    {
        $result = NULL;
    }
%}
%typemap(javaout) retval { return $jnicall; }

%template(StringVec) std::vector<std::string>;

%inline%{

struct RemoteNode
{
    std::string address;
    unsigned short port;
};

RemoteNode qdb_get_location(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    qdb_remote_node_t remote;
    err->error = qdb_get_location(handle, alias, &remote);

    RemoteNode location;
    if (err->error != qdb_e_ok)
    {
        return location;
    }

    location.address = std::string(remote.address);
    location.port = remote.port;

    qdb_free_buffer(handle, remote.address);

    return location;
}

retval qdb_node_status(qdb_handle_t handle, const char * uri, error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_node_status(handle, uri, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

retval qdb_node_config(qdb_handle_t handle, const char * uri, error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_node_config(handle, uri, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

retval qdb_node_topology(qdb_handle_t handle, const char * uri, error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_node_topology(handle, uri, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

%}

%apply const char * content { const char * comparand };

%typemap(in) (char *content, size_t content_length) {
    /* %typemap(in) (char * content, size_t content_length) */
    $1 = ($1_ltype)jenv->GetDirectBufferAddress($input);
    if ($1 == 0)
    {
        SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get the address of direct buffer. Buffer must be allocated direct.");
    }
    $2 = ($2_ltype)jenv->GetDirectBufferCapacity($input);
    if ($2 == -1)
    {
        SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get the capacity of direct buffer. Buffer must be allocated direct.");
    }
}

// we need to do this otherwise SWIG will try to release the ByteBuffer as if it were a String
%typemap(freearg, noblock=1) char * content {}

 /* These 3 typemaps tell SWIG what JNI and Java types to use */
%typemap(jni) (char *content, size_t content_length) "jobject"
%typemap(jtype) (char *content, size_t content_length) "java.nio.ByteBuffer"
%typemap(jstype) (char *content, size_t content_length) "java.nio.ByteBuffer"
%typemap(javain) (char *content, size_t content_length) "$javainput"
%typemap(javaout) (char *content, size_t content_length) { return $jnicall; }

%inline%{
    void qdb_free_buffer(qdb_handle_t handle, char * content, size_t content_length)
    {
        qdb_free_buffer(handle, content);
    }
%}

namespace qdb
{
std::string make_error_string(qdb_error_t error);
}

qdb_error_t qdb_remove(qdb_handle_t handle,  const char * alias);

qdb_error_t qdb_purge_all(qdb_handle_t handle);

qdb_error_t qdb_trim_all(qdb_handle_t handle);

%include "qdb_api_iterator.i"

// expiry functions
qdb_error_t qdb_expires_at(qdb_handle_t handle, const char * alias, qdb_time_t expiry_time);

qdb_error_t qdb_expires_from_now(qdb_handle_t handle, const char * alias, qdb_time_t expiry_delta);

%inline%{
// need a "direct buffer" version to access the content to avoid building needless strings
qdb_time_t qdb_get_expiry(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    qdb_time_t res = 0;
    err->error = qdb_get_expiry_time(handle, alias, &res);
    return res;
}
%}

%include "qdb_api_batch.i"
%include "qdb_api_blob.i"
%include "qdb_api_integer.i"
%include "qdb_api_deque.i"
%include "qdb_api_hset.i"
%include "qdb_api_tag.i"
%include "qdb_api_stream.i"
