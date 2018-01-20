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
        return new Query(query);
    }

    public Result execute(QdbSession session) {
        if (this.query == null) {
            throw new QdbInputException("Cannot execute an empty query");
        }

        Reference<Result> result = new Reference<Result>();

        int err = qdb.query_execute(session.handle(), this.query, result);
        QdbExceptionFactory.throwIfError(err);

        return result.value;
    }
}
