import java.time.Instant;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.stream.Stream;
import java.nio.file.Paths;
import java.nio.file.Files;
import org.json.JSONObject;

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
        Tutorial.bulkRead(c);
        Tutorial.query(c);

        Tutorial.dropTable(c);
    }

    private void secureConnect() {
        String username = "";
        String user_secret_key = "";
        String cluster_public_key = "";
        try {
            String user_file_content = new String(Files.readAllBytes(Paths.get("user_private.key")));
            JSONObject user = new JSONObject(user_file_content);
            username = user.get("username").toString();
            user_secret_key = user.get("secret_key").toString();
            cluster_public_key = new String(Files.readAllBytes(Paths.get("cluster_public.key")));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        // secure-connect-start
        Session c;
        try {
            c = Session.connect(new Session.SecurityOptions(username,
                                                            user_secret_key,
                                                            cluster_public_key),
                                "qdb://127.0.0.1:2838");
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
            c = Session.connect("qdb://127.0.0.1:2836");
        } catch (ConnectionRefusedException ex) {
            System.err.println("Failed to connect to cluster, make sure server is running!");
            throw ex;
        }

        // connect-end

        return c;
    }

    private static void poolConnect() throws ConnectionRefusedException, IOException, java.lang.InterruptedException {
        // pool-connect-start
        SessionFactory factory = new SessionFactory("qdb://127.0.0.1:2836");

        // Create a static-sized pool with 16 connections pre-allocated
        SessionPool pool = new SessionPool(factory, 16);

        Session s = pool.acquire();
        try {
            // Do something with `s`
        } finally {

            // Always make sure to properly release the connection back to the pool
            pool.release(s);
        }

        // pool-connect-end
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

        // tags-start

        // You can also attach a tag by only providing the table string. See the
        // javadocs for other ways to call this function.

        Table.attachTag(c, t, "nasdaq");

        // tags-end

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
        w.append(new Timespec(Instant.ofEpochSecond(1548979200)),
                 new Value[] {
                     Value.createDouble(3.40),
                     Value.createDouble(3.50),
                     Value.createInt64(10000)
                 });

        // Inserting the next row is a matter of just calling append.
        w.append(new Timespec(Instant.ofEpochSecond(1549065600)),
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

    private static void bulkRead(Session c) throws IOException {
        // bulk-read-start

        // We first initialize the TimeRange we are looking for. Providing a timerange
        // to a bulk reader is mandatory.
        TimeRange[] ranges = new TimeRange[] { new TimeRange(new Timespec(Instant.ofEpochSecond(1548979200)),
                                                             new Timespec(Instant.ofEpochSecond(1549065600))) };

        // In this example, we initialize a bulk reader by simply providing a session,
        // table name and timerange we're interested in. For alternative ways to initialize
        // a bulk reader, please refer to the javadoc of the Table class.
        Reader r = Table.reader(c, "stocks", ranges);

        // The reader implements an Iterator interface which allows us to traverse the rows:
        while (r.hasNext()) {

            WritableRow row = r.next();

            // Each row has a timestamp which you can access as a Timespec:
            System.out.println("row timestamp: " + row.getTimestamp().toString());

            // Note that the offsets of the values array align with the offsets we used
            // when creating the table, i.e. 0 means "open", 1 means "close" and 2 means
            // "volume":
            Value[] values = row.getValues();

            Value openValue = values[0];
            Value closealue = values[1];
            Value volumeValue = values[2];
        }

        // bulk-read-end
    }


    private static void query(Session c) throws IOException {
        // query-start

        // We can either construct a query from a raw string like this
        Query q1 = Query.of("SELECT SUM(volume) FROM stocks");

        // Or we can use the QueryBuilder for more flexible query building, especially
        // useful for providing e.g. ranges.
        String colName = "volume";
        String tableName = "stocks";

        Query q2 = new QueryBuilder()
            .add("SELECT SUM(")
            .add(colName)
            .add(") FROM")
            .add(tableName)
            .asQuery();

        // Execute the query
        Result r = q1.execute(c);

        // In this case, columns[0] matches to result rows[0] and are
        // our timestamps.
        String[] columns = r.columns;
        Row[] rows = r.rows;

        // Last but not least, the Query results API also implements native Java
        // streams.
        Stream<Row> s = r.stream();

        System.out.println("total row count: " + s.count());

        // query-end
    }

    private static void dropTable(Session c) throws IOException {
        // drop-table-start

        // We can simply remove a table by its name.

        Table.remove(c, "stocks");

        // drop-table-end
    }
}
