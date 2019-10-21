import java.time.Instant;
import java.io.IOException;
import java.util.stream.Stream;

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
        // secure-connect-start
        Session c;
        try {
            c = Session.connect(new Session.SecurityOptions("user_name",
                                                            // User private key
                                                            "SL8sm9dM5xhPE6VNhfYY4ib4qk3vmAFDXCZ2FDi8AuJ4=",
                                                            // Cluster public key
                                                            "PZMBhqk43w+HNr9lLGe+RYq+qWZPrksFWMF1k1UG/vwc="),
                                "qdb://127.0.0.1:28362");
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

            Row row = r.next();

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

        // We can either access the stock result class directly like this
        Result.Table stocksResult = r.tables[0];

        // In this case, columns[0] matches to result rows[0] and are
        // our timestamps.
        String[] columns = stocksResult.columns;
        Value[][] rows = stocksResult.rows;

        // If you would prefer to access the query results in a row-oriented
        // way, you can flatten the result into Rows.
        Result.Row[] flattened = stocksResult.flatten();

        // Last but not least, the Query results API also implements native Java
        // streams.
        Stream<Result.Row> s = r.stream();

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