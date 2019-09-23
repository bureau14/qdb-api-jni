import java.io.IOException;

// import-start
import net.quasardb.qdb.*;
import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.*;
// import-end

public class Tutorial {

    public static void main(String[] args) throws IOException {

        // connect-start
        Session c;

        try {
            c = Session.connect("qdb://127.0.0.1:28360");
        } catch (ConnectionRefusedException ex) {
            System.err.println("Failed to connect to cluster, make sure server is running!");
            System.exit(1);
        }
        // connect-end
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

}
