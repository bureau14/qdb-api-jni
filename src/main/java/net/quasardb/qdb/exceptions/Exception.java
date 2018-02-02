package net.quasardb.qdb.exception;

/**
 * Exception thrown when an entry cannot be found in the database
 */
public class Exception extends RuntimeException {

    public Exception(String message) {
        super(message);
    }
}
