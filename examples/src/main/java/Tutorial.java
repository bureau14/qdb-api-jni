import java.util.Date;
import java.io.IOException;

// import-start
import net.quasardb.qdb.*;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;
// import-end


public class Tutorial {

    public static void main(String[] args) throws IOException {
        Session c = Tutorial.connect();
        Table t = Tutorial.createTable(c);
        Tutorial.batchInsert(c);
    }

    private void secureConnect() {
        // secure-connect-start
        Session c;
        try {
            c = Session.connect(new Session.SecurityOptions("user_name",
                                                            // User private key
                                                            "SL8sm9dM5xhPE6VNhfYY4ib4qk3vmAFDXCZ2FDi8AuJ4=",
                                                            // Cluster public key
                                                            "PZMBhqk43w+HNr9lLGe+RYq+qWZPrksFWMF1k1UG/vwc="),
                                "qdb://127.0.0.1:28361");
        } catch (ConnectionRefusedException ex) {
            System.err.println("Failed to connect to cluster, make sure server is running!");
            System.exit(1);
        }
        // secure-connect-end

    }

    private static Session connect() throws ConnectionRefusedException {
        // connect-start
        Session c;

        try {
            c = Session.connect("qdb://127.0.0.1:28360");
        } catch (ConnectionRefusedException ex) {
            System.err.println("Failed to connect to cluster, make sure server is running!");
            throw ex;
        }

        // connect-end

        return c;
    }

    private static Table createTable(Session c) {
        // create-table-start

        Column[] definitions = {
            new Column.Double ("open"),
            new Column.Double ("close"),
            new Column.Int64 ("volume")
        };

        // This will return a reference to the newly created Table
        Table t = Table.create(c, "stocks", definitions);

        // create-table-end

        return t;
    }

    private static void batchInsert(Session c) throws IOException {
        // batch-insert-start

        // We initialize a Writer here that automatically flushes rows as we insert
        // them, by default every 50,000 rows. If we want to explicitly control these
        // flushes, use `Table.writer()` instead.
        Writer w = Table.autoFlushWriter(c, "stocks");

        // Insert the first row: to start a new row, we must provide it with a mandatory
        // timestamp that all values for this row will share. QuasarDB will use this timestamp
        // as its primary index.
        w.append(new Timespec(new Date(2019, 02, 01).toInstant()), // Converts local time to UTC!
                 new Value[] {
                     Value.createDouble(3.40),
                     Value.createDouble(3.50),
                     Value.createInt64(10000)
                 });

        // Inserting the next row is a matter of just calling append.
        w.append(new Timespec(new Date(2019, 02, 02).toInstant()),  // Converts local time to UTC!
                 new Value[] {
                     Value.createDouble(3.50),
                     Value.createDouble(3.55),
                     Value.createInt64(7500)
                 });


        // Now that we're done, we push the buffer as one single operation. Note that,
        // because in this specific example we are using the autoFlushWriter, this would
        // happen automatically under the hood every append() invocations.
        w.flush();

        // batch-insert-end
    }
}
