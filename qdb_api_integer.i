qdb_error_t qdb_int_put(qdb_handle_t handle, const char * alias, qdb_int_t integer, qdb_time_t expiry_time);

qdb_error_t qdb_int_update(qdb_handle_t handle, const char * alias, qdb_int_t integer, qdb_time_t expiry_time);

%inline%{

qdb_int_t qdb_int_get(qdb_handle_t handle, const char * alias, error_carrier * err)
{
    qdb_int_t res;
    err->error = qdb_int_get(handle, alias, &res);
    return res;
}

qdb_int_t qdb_int_add(qdb_handle_t handle, const char * alias, qdb_int_t addend, error_carrier * err)
{
    qdb_int_t res;
    err->error = qdb_int_add(handle, alias, addend, &res);
    return res;
}

%}
