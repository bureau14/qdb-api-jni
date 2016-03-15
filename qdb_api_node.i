qdb_error_t qdb_stop_node(
    qdb_handle_t handle,
    const char * uri,
    const char * reason);

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