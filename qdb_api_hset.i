qdb_error_t qdb_hset_insert(qdb_handle_t handle, const char * alias, const char * content, size_t content_length);

qdb_error_t qdb_hset_erase(qdb_handle_t handle, const char * alias, const char * content, size_t content_length);

qdb_error_t qdb_hset_contains(qdb_handle_t handle, const char * alias, const char * content, size_t content_length);
