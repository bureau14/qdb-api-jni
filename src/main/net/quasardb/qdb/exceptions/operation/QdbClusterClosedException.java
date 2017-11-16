package net.quasardb.qdb;

/**
 * Exception thrown when trying to perform an operation on a closed QdbCluster
 */
public final class QdbClusterClosedException extends QdbOperationException {

    public QdbClusterClosedException() {
        super("Operation cannot be performed because QdbCluster.close() has been called");
    }
}