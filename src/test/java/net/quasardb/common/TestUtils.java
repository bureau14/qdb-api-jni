package net.quasardb.common;

import net.quasardb.qdb.Session;

public class TestUtils {
    public static final String CLUSTER_URI = "qdb://127.0.0.1:28360";

    public static Session createSession() {
        return Session.connect(CLUSTER_URI);
    }
}
