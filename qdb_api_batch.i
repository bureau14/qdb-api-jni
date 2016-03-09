%template(BatchOpsVec) std::vector<qdb_operation_t>;

%inline%{

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

%}
