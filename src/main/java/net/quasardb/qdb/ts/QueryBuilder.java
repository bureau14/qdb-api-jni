package net.quasardb.qdb.ts;

import java.util.StringJoiner;

/**
 * Utility class with functions that make it easier to construct
 * a query.
 */
public final class QueryBuilder {
    private StringJoiner query;

    public QueryBuilder() {
        this.query = new StringJoiner(" ");
    }

    private QueryBuilder(StringJoiner query) {
        this.query = query;
    }

    /**
     * Adds plain java string to query.
     */
    public QueryBuilder add(String str) {
        return new QueryBuilder(this.query.add(str));
    }

    /**
     * Adds clause that limits a query's results to a
     * timerange.
     */
    public QueryBuilder in(TimeRange range) {
        return
            add("in range (")
            .add(range.getBegin().asInstant().toString())
            .add(",")
            .add(range.getEnd().asInstant().toString())
            .add(")");
    }

    public Query asQuery() {
        return Query.of(this.toString());
    }

    public String toString() {
        return this.query.toString();
    }
}
