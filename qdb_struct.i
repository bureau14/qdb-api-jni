struct qdb_const_iterator_t
{
    %immutable;
    qdb_handle_t handle;            /* [in] */
    const void * token;             /* [in] */

    const void * node;              /* [out] */
    const void * ref;               /* [out] */

    // we get rid of the const otherwise SWIG might leak memory
    char * alias;                   /* [out] */
    char * content;                 /* [out] */
    size_t  content_size;           /* [out] */
    %mutable;
} ;

struct qdb_remote_node_t
{
    // we get rid of the const otherwise SWIG might leak memory
    char * address;                 /* [in] */
    unsigned short port;            /* [in] */
};

%inline%{

struct retval
{
    retval(void) : buffer(0), buffer_size(0) {}

    char * buffer;
    size_t buffer_size;
};

struct error_carrier
{
    error_carrier(void) : error(qdb_e_ok) {}

    qdb_error_t error;
};

struct results_list
{
    results_list(void) : error(qdb_e_uninitialized) {}

    qdb_error_t error;
    std::vector<std::string> results;
};

%}
