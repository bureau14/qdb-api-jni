package net.quasardb.qdb.exception;

/**
 * Exception thrown when an operation on an iterator is considered invalid, i.e.
 * while attempting to dereference an iterator that is pointing to the end of a
 * collection.
 */
public final class InvalidIteratorException extends OperationException {

    public InvalidIteratorException(String message) {
        super(message);
    }
}
