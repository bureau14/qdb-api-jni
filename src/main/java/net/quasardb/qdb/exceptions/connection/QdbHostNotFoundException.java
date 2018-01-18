package net.quasardb.qdb;

/**
 * Exception thrown when the host name resolution fails.
 */
public final class QdbHostNotFoundException extends QdbConnectionException {

    public QdbHostNotFoundException() {
        super("The remote host cannot be resolved.");
    }
}
