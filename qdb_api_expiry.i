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