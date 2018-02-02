package net.quasardb.qdb.exception;

/**
 * Exception thrown when the connection to the database caused an error
 */
public class ConnectionException extends Exception {

    public ConnectionException(String message) {
        super(message);
    }
}
