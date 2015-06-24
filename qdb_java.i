%module(package="qdb") qdb
#pragma SWIG nowarn=453

%typemap(jni)       qdb_time_t "jlong"
%typemap(jtype)     qdb_time_t "long"
%typemap(jstype)    qdb_time_t "long"
%typemap(javain)    qdb_time_t "$javainput"
// we need to force the mapping of qdb_time_t to a long otherwise swig tries to create an intermediate object
// and checks that the object isn't null while qdb_time_t can be zero
%typemap(in)        qdb_time_t %{ $1 = $input; %}
%typemap(out)       qdb_time_t  %{ $result = $1; %}
%typemap(javaout)   qdb_time_t { return $jnicall; }


%typemap(jni)       qdb_int "jlong"
%typemap(jtype)     qdb_int "long"
%typemap(jstype)    qdb_int "long"
%typemap(javain)    qdb_int "$javainput"

%typemap(in)        qdb_int %{ $1 = $input; %}
%typemap(out)       qdb_int  %{ $result = $1; %}
%typemap(javaout)   qdb_int { return $jnicall; }

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

%typemap(jni)       void * BUFFER "jobject"
%typemap(jtype)     void * BUFFER "java.nio.ByteBuffer"
%typemap(jstype)    void * BUFFER "java.nio.ByteBuffer"
%typemap(javain)    void * BUFFER "$javainput"

// we need to do this otherwise SWIG will try to release the ByteBuffer as if it were a String
%typemap(memberin)  void * BUFFER "$1 = $input;"
%typemap(freearg, noblock=1)   void * BUFFER ""
%typemap(javaout)   void * BUFFER { return $jnicall; }

%apply void * BUFFER { char * content };
// we have false warning because of size_t -> long
// and there is no unsigned type in Java
%apply void * BUFFER { char * comparand };
%apply void * BUFFER { char * result };

%apply unsigned long { size_t };
%apply unsigned long { qdb_time_t };

%typemap(in) char * content %{
    if ($input)
    {
        arg1->content_size = jenv->GetDirectBufferCapacity($input);
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
    if ($input)
    {
        arg1->comparand_size = jenv->GetDirectBufferCapacity($input);
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

qdb_error_t
qdb_put(
        qdb_handle_t handle,   /* [in] API handle */
        const char * alias,       /* [in] unique identifier for new entry */
        const char * content,     /* [in] content for new entry */
        size_t content_length,     /* [in] size of content, in bytes */
        qdb_time_t expiry_time
    );

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

// specific to java getter
retval qdb_get(qdb_handle_t handle,  const char * alias, error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_get(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

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

retval qdb_get_and_remove(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_get_and_remove(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

retval qdb_get_and_update(qdb_handle_t handle,
    const char * alias,          /* [in] unique identifier of existing entry */
    const char * content,        /* [in] new content for entry */
    size_t content_length,       /* [in] size of content, in bytes */
    qdb_time_t expiry_time,
    error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_get_and_update(handle, alias, content, content_length, expiry_time, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
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

%inline%{

retval qdb_compare_and_swap(qdb_handle_t handle,   /* [in] API handle */
    const char * alias,                            /* [in] unique identifier of existing entry */
    const char * content,                          /* [in] new content for entry */
    size_t content_length,                         /* [in] size of content, in bytes */
    const char * comparand,                        /* [in] comparand for entry */
    size_t comparand_length,                       /* [in] size of comparand, in bytes */
    qdb_time_t expiry_time,
    error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_compare_and_swap(handle, alias, content, content_length, comparand, comparand_length, expiry_time, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

%}

%typemap(in) (char *content, size_t content_length) {
  /* %typemap(in) void * */
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
%typemap(freearg, noblock=1) char * content {  }

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

qdb_error_t
qdb_update(
        qdb_handle_t handle,   /* [in] API handle */
        const char * alias,       /* [in] unique identifier of existing entry */
        const char * content,     /* [in] new content for entry */
        size_t content_length,     /* [in] size of content, in bytes */
        qdb_time_t expiry_time
    );

qdb_error_t qdb_remove(qdb_handle_t handle,  const char * alias);

qdb_error_t
qdb_remove_if(
    qdb_handle_t handle,                /* [in] API handle */
    const char * alias,                 /* [in] unique identifier of existing entry */
    const char * comparand,             /* [in] comparand for entry */
    size_t comparand_length           /* [in] size of comparand, in bytes */
);


qdb_error_t qdb_purge_all(qdb_handle_t handle);

// iterating functions
qdb_error_t qdb_iterator_begin(qdb_handle_t handle, qdb_const_iterator_t * iterator);
qdb_error_t qdb_iterator_rbegin(qdb_handle_t handle, qdb_const_iterator_t * iterator);
qdb_error_t qdb_iterator_next(qdb_const_iterator_t * iterator);
qdb_error_t qdb_iterator_previous(qdb_const_iterator_t * iterator);
qdb_error_t qdb_iterator_close(qdb_const_iterator_t * iterator);
qdb_error_t qdb_iterator_copy(const qdb_const_iterator_t * original, qdb_const_iterator_t * copy);

%inline%{

// need a "direct buffer" version to access the content to avoid building needless strings
retval qdb_iterator_content(const qdb_const_iterator_t * iterator)
{
    retval res;

    if (iterator->handle && iterator->token && iterator->alias)
    {
        res.buffer = const_cast<char *>(iterator->content);
        res.buffer_size = iterator->content_size;
    }

    return res;
}

%}

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

%template(BatchOpsVec) std::vector<qdb_operation_t>;

%inline%{

run_batch_result run_batch(qdb_handle_t h, const std::vector<qdb_operation_t> & requests)
{
    run_batch_result br;

    // transform the batch request in to a qdb_operation_t
    // it's safe because the strings are kept alive by our vector passed by const reference
    br.results.resize(requests.size());
    std::copy(requests.begin(), requests.end(), br.results.begin());

    // yes, this is legal and safe
    br.successes = qdb_run_batch(h, &br.results[0], br.results.size());

    return br;
}

void release_batch_result(qdb_handle_t h, run_batch_result & br)
{
    qdb_free_operations(h, &br.results[0], br.results.size());
}

%}

// integer functions
qdb_error_t qdb_int_put(qdb_handle_t handle, const char * alias, qdb_int integer, qdb_time_t expiry_time);
qdb_error_t qdb_int_update(qdb_handle_t handle, const char * alias, qdb_int integer, qdb_time_t expiry_time);

%inline%{

qdb_int qdb_int_get(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    qdb_int res;

    err->error = qdb_int_get(handle, alias, &res);

    return res;
}

qdb_int qdb_int_add(qdb_handle_t handle, const char * alias, qdb_int addend, error_carrier * err)
{
    qdb_int res;

    err->error = qdb_int_add(handle, alias, addend, &res);

    return res;
}

%}
// queue functions

qdb_error_t qdb_queue_push_front(qdb_handle_t handle,  const char * alias,  const char * content, size_t content_length);
qdb_error_t qdb_queue_push_back(qdb_handle_t handle,   const char * alias,  const char * content, size_t content_length);

%inline%{

retval qdb_queue_pop_front(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_queue_pop_front(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

retval qdb_queue_pop_back(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_queue_pop_back(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

retval qdb_queue_front(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_queue_front(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

retval qdb_queue_back(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    retval res;
    const char * buf = res.buffer;
    err->error = qdb_queue_back(handle, alias, &buf, &res.buffer_size);
    if (err->error == qdb_e_ok)
    {
        res.buffer = const_cast<char *>(buf);
    }
    return res;
}

%}

// set functions
qdb_error_t qdb_hset_insert(qdb_handle_t handle, const char * alias, const char * content, size_t content_length);
qdb_error_t qdb_hset_erase(qdb_handle_t handle, const char * alias, const char * content, size_t content_length);
qdb_error_t qdb_hset_contains(qdb_handle_t handle, const char * alias, const char * content, size_t content_length);

// tag functions
qdb_error_t qdb_add_tag(qdb_handle_t handle, const char * alias, const char * tag);
qdb_error_t qdb_has_tag(qdb_handle_t handle, const char * alias, const char * tag);
qdb_error_t qdb_remove_tag(qdb_handle_t handle, const char * alias, const char * tag);


%inline%{

    template <typename T>
    struct cstring_translator
    {
        T operator()(const char * cstr) const
        {
            return T(cstr);
        }
    };

    static qdb_error_t __transform_results(const char ** results, size_t results_count, std::vector<std::string> & final_result)
    {
        qdb_error_t err = qdb_e_uninitialized;

        try
        {
            final_result.resize(results_count);
            std::transform(results, results + results_count, final_result.begin(), cstring_translator<std::string>());
        }
        catch (const std::bad_alloc &)
        {
            final_result.clear();
            return qdb_e_no_memory;
        }

        return qdb_e_ok;
    }

    results_list qdb_get_tagged(qdb_handle_t handle, const char * tag)
    {
        results_list res;

        const char ** aliases = NULL;
        size_t aliases_count = 0;

        res.error = qdb_get_tagged(handle, tag, &aliases, &aliases_count);
        if (res.error == qdb_e_ok)
        {
            res.error = __transform_results(aliases, aliases_count, res.results);
        }

        qdb_free_results(handle, aliases, aliases_count);

        return res;
    }

    results_list qdb_get_tags(qdb_handle_t handle, const char * alias)
    {
        results_list res;

        const char ** tags = NULL;
        size_t tags_count = 0;

        res.error = qdb_get_tags(handle, alias, &tags, &tags_count);
        if (res.error == qdb_e_ok)
        {
            res.error = __transform_results(tags, tags_count, res.results);
        }

        qdb_free_results(handle, tags, tags_count);

        return res;
    }
%}
