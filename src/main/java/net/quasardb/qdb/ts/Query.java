package net.quasardb.qdb.ts;

import java.util.Optional;

import net.quasardb.qdb.Session;
import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.Reference;
import net.quasardb.qdb.exception.ExceptionFactory;
import net.quasardb.qdb.exception.InputException;

/**
 * Represents a timeseries query.
 *
 * Use this class directly if you're planning on writing complex, custom
 * queries. If your queries are relatively simple, consider using the
 * {@link QueryBuilder} instead.
 *
 * @see QueryBuilder
 */
public final class Query {

    private String query;

    protected Query() {
    }

    protected Query(String query) {
        this.query = query;
    }

    /**
     * Creates a new, empty query instance.
     */
    public static Query create() {
        return new Query();
    }

    /**
     * Returns new Query instance based on a string.
     *
     * @param query The query string to execute. Refer to the QuasarDB documentation
     *              for the full query syntax.
     */
    public static Query of(String query) {
        System.out.println("executing query: " + query);
        return new Query(query);
    }

    public Result execute(Session session) {
        if (this.query == null) {
            throw new InputException("Cannot execute an empty query");
        }

        Reference<Result> result = new Reference<Result>();

        int err = qdb.query_execute(session.handle(), this.query, result);
        ExceptionFactory.throwIfError(err);

        return result.value;
    }
}
