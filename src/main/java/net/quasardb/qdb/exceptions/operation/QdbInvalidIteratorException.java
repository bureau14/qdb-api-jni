package net.quasardb.qdb;

/**
 * Exception thrown when an operation on an iterator is considered invalid, i.e.
 * while attempting to dereference an iterator that is pointing to the end of a
 * collection.
 */
public final class QdbInvalidIteratorException extends QdbOperationException {

    public QdbInvalidIteratorException() {
        super("The operation being performed is referencing an invalid iterator.");
    }
}
