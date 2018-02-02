package net.quasardb.qdb.exception;

/**
 * Exception thrown when trying to perform an operation on a closed Buffer
 */
public final class BufferClosedException extends OperationException {

    public BufferClosedException() {
        super("Operation cannot be performed because Buffer.close() was called");
    }
}
