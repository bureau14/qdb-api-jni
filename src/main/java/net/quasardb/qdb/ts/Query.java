package net.quasardb.qdb.ts;

import java.util.Optional;

import net.quasardb.qdb.jni.qdb;
import net.quasardb.qdb.jni.Reference;
import net.quasardb.qdb.QdbInputException;
import net.quasardb.qdb.QdbSession;
import net.quasardb.qdb.QdbExceptionFactory;

/**
 * Represents a timeseries query.
 */
public final class Query {

    private String query;
    private Optional<Result> result;

    protected Query() {
    }

    protected Query(String query) {
        this.query = query;
        this.result = Optional.empty();
    }

    protected Query(String query, Result result) {
        this.query = query;
        this.result = Optional.of(result);
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
        return new Query(query);
    }
    /**
     * Returns a copy of this query object that reuses an existing
     * Result's memory.
     */
    public Query withResult(Result result) {
        return new Query(this.query, result);
    }

    public Result execute(QdbSession session) {
        if (this.query == null) {
            throw new QdbInputException("Cannot execute an empty query");
        }

        System.out.println("executing query: " + this.query);

        Reference<Result> result =
            Reference.of(this.result.orElse(new Result()));

        int err = qdb.query_execute(session.handle(), this.query, result);
        QdbExceptionFactory.throwIfError(err);

        return result.value;
    }
}
