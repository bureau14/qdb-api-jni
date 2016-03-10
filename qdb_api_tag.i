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
            return qdb_e_no_memory_local;
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
