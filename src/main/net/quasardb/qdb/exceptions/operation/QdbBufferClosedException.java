package net.quasardb.qdb;

/**
 * Exception thrown when trying to perform an operation on a closed QdbBuffer
 */
public final class QdbBufferClosedException extends QdbOperationException {

    public QdbBufferClosedException() {
        super("Operation cannot be performed because QdbBuffer.close() was called");
    }
}