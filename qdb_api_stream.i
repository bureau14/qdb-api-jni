%{
#include <qdb/stream.h>
%}

typedef struct qdb_stream_session * qdb_stream_t;
%nodefaultdtor qdb_stream_t;

%typemap(jni)       qdb_stream_size_t "jlong"
%typemap(jtype)     qdb_stream_size_t "long"
%typemap(jstype)    qdb_stream_size_t "long"
%typemap(javain)    qdb_stream_size_t "$javainput"
%typemap(in)        qdb_stream_size_t %{ $1 = $input; %}
%typemap(out)       qdb_stream_size_t  %{ $result = $1; %}
%typemap(javaout)   qdb_stream_size_t { return $jnicall; }

%typemap(jni)       const qdb_stream_size_t * "jlong"
%typemap(jtype)     const qdb_stream_size_t * "long"
%typemap(jstype)    const qdb_stream_size_t * "long"
%typemap(javain)    const qdb_stream_size_t * "$javainput"
%typemap(in)        const qdb_stream_size_t * %{ $1 = (qdb_stream_size_t *)&$input; %}
%typemap(out)       const qdb_stream_size_t *  %{ $result = *$1; %}
%typemap(javaout)   const qdb_stream_size_t * { return $jnicall; }

%typemap(jni)     const void * content "jobject"
%typemap(jtype)   const void * content "java.nio.ByteBuffer"
%typemap(jstype)  const void * content "java.nio.ByteBuffer"
%typemap(javain)  const void * content "$javainput"
%typemap(javaout) const void * content { return $jnicall; }
%typemap(in) const void * content {
    /* %typemap(in) const void * content */
    $1 = ($1_ltype)jenv->GetDirectBufferAddress($input);
    if ($1 == 0)
    {
        SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get the address of direct buffer. Buffer must be allocated direct.");
    }
}

qdb_error_t qdb_stream_write(qdb_stream_t stream, const void * content, size_t size);

%typemap(jni)     char * BUFFER "jobject"
%typemap(jtype)   char * BUFFER "java.nio.ByteBuffer"
%typemap(jstype)  char * BUFFER "java.nio.ByteBuffer"
%typemap(javain)  char * BUFFER "$javainput"
%typemap(javaout) char * BUFFER { return $jnicall; }
// we need to do this otherwise SWIG will try to release the ByteBuffer as if it were a String
%typemap(freearg, noblock=1) char * BUFFER {}
%typemap(in) char * BUFFER {
    /* %typemap(in) char * BUFFER */
    $1 = ($1_ltype)jenv->GetDirectBufferAddress($input);
    if ($1 == 0)
    {
        SWIG_JavaThrowException(jenv, SWIG_JavaRuntimeException, "Unable to get the address of direct buffer. Buffer must be allocated direct.");
    }
}

enum qdb_stream_mode_t
{
    qdb_stream_mode_read = 0,
    qdb_stream_mode_append = 1,
};

%inline%{

qdb_stream_t qdb_stream_open(qdb_handle_t handle, const char * alias, qdb_stream_mode_t mode, error_carrier * err)
{
    qdb_stream_t s;
    err->error = qdb_stream_open(handle, alias, mode, &s);
    return s;
}

qdb_error_t qdb_stream_read(qdb_stream_t stream, char * BUFFER, int * INOUT)
{
    size_t sz = static_cast<size_t>(*INOUT);
    qdb_error_t err = qdb_stream_read(stream, BUFFER, &sz);
    *INOUT = static_cast<int>(sz);
    return err;
}

qdb_error_t qdb_stream_size(qdb_stream_t stream, long long * OUTPUT)
{
    qdb_stream_size_t sz = 0u;
    qdb_error_t err = qdb_stream_size(stream, &sz);
    *OUTPUT = static_cast<long long>(sz);
    return err;
}

qdb_error_t qdb_stream_getpos(qdb_stream_t stream, long long * OUTPUT)
{
    qdb_stream_size_t pos = 0u;
    qdb_error_t err = qdb_stream_getpos(stream, &pos);
    *OUTPUT = static_cast<long long>(pos);
    return err;
}

%}

qdb_error_t qdb_stream_close(qdb_stream_t stream);
qdb_error_t qdb_stream_setpos(qdb_stream_t stream, qdb_stream_size_t position);
qdb_error_t qdb_stream_truncate(qdb_stream_t stream, qdb_stream_size_t position);
