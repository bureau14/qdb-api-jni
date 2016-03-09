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