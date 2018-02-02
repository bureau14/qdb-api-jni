package net.quasardb.qdb.exception;

/**
 * Exception thrown when the connection to the cluster is refused.
 */
public final class ConnectionRefusedException extends ConnectionException {

    public ConnectionRefusedException() {
        super("Connection refused.");
    }
}
