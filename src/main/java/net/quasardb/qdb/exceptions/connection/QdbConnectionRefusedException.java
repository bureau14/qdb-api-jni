package net.quasardb.qdb;

/**
 * Exception thrown when the connection to the cluster is refused.
 */
public final class QdbConnectionRefusedException extends QdbConnectionException {

    public QdbConnectionRefusedException() {
        super("Connection refused.");
    }
}
