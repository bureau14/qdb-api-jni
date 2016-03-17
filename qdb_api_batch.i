struct qdb_operation_t
{
    qdb_operation_type_t type;      /* [in] */

    // we get rid of the const otherwise SWIG might leak memory
    char * alias;                   /* [in] */

    char * content;                  /* [in] */
    size_t content_size;            /* [in] */

    char * comparand;               /* [in] */
    size_t comparand_size;          /* [in] */

    qdb_time_t expiry_time;         /* [in] */

    %immutable;
    qdb_error_t error;              /* [out] */

    char * result;                  /* [out] API allocated */
    size_t result_size;             /* [out] */
    %mutable;
};

%extend qdb_operation_t {
    qdb_operation_t() {
        qdb_operation_t *that = new qdb_operation_t();
        that->error = qdb_e_uninitialized;
        return that;
    }
};

%template(BatchOpsVec) std::vector<qdb_operation_t>;

%inline%{

struct run_batch_result
{
    run_batch_result(void) : successes(0) {}

    size_t successes;
    std::vector<qdb_operation_t> results;
};

run_batch_result run_batch(qdb_handle_t h, const std::vector<qdb_operation_t> & requests)
{
    run_batch_result br;

    // transform the batch request in to a qdb_operation_t
    // it is safe because the strings are kept alive by our vector passed by const reference
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

jlong run_batch2(qdb_handle_t h, std::vector<qdb_operation_t> & operations)
{
    if (operations.size() > 0)
    {
        return qdb_run_batch(h, &operations[0], operations.size());
    }
    else
    {
        return 0;
    }
}

void free_operations(qdb_handle_t h, std::vector<qdb_operation_t> & operations)
{
    if (operations.size() > 0)
    {
        qdb_free_operations(h, &operations[0], operations.size());
        operations.clear();
    }
}

%}
