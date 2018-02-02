package net.quasardb.qdb.exception;

/**
 * Exception thrown when trying to perform an operation on a closed Cluster
 */
public final class ClusterClosedException extends OperationException {

    public ClusterClosedException() {
        super("Operation cannot be performed because Cluster.close() has been called");
    }
}