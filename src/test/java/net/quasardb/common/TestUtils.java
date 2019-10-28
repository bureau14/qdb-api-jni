package net.quasardb.common;

import java.io.IOException;
import java.util.stream.Stream;
import java.util.UUID;

import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;

public class TestUtils {
    public static final String CLUSTER_URI = "qdb://127.0.0.1:28360";

    public static Session createSession() {
        return Session.connect(CLUSTER_URI);
    }

    public static Column[] generateTableColumns(int count) {
        return generateTableColumns(Value.Type.DOUBLE, count);
    }

    public static Column[] generateTableColumns(Value.Type valueType, int count) {
        return Stream.generate(TestUtils::createUniqueAlias)
            .limit(count)
            .map((alias) -> {
                    return new Column(alias, valueType);
                })
            .toArray(Column[]::new);
    }

    public static Table createTable(Column[] columns) throws IOException {
        return createTable(createSession(), columns);
    }

    public static Table createTable(Session s, Column[] columns) throws IOException {
        return Table.create(s, createUniqueAlias(), columns);
    }

    public static String createUniqueAlias() {
        return new String("a") + UUID.randomUUID().toString().replaceAll("-", "");


    }

}
