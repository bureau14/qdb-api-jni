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

}
