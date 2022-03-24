package net.quasardb.qdb.ts;

import java.util.*;
import java.time.*;
import java.lang.Exception;
import java.util.stream.Stream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.*;

import net.quasardb.common.TestUtils;
import net.quasardb.qdb.ts.*;
import net.quasardb.qdb.*;
import net.quasardb.qdb.exception.InvalidIteratorException;
import net.quasardb.qdb.exception.InvalidArgumentException;

@State(Scope.Benchmark)
@Threads(1)
public class WriterBenchmark {

    // @Param({"FIRST", "MANY"})
    @Param({"FIRST"})
    public String tableSpread;

    // @Param({"pinnedWriter", "expWriter"})
    @Param({"expWriter"})
    public String writerType;

    @Param({"APPEND", "FLUSH"})
    public String operationType;

    @Param({"10000"})
    public int tableCount;

    @Param({"5"})
    public int columnCount;

    @Param({"10000"})
    public int rowCount;

    @Param({"ASYNC"})
    public Writer.PushMode pushMode;

    @Param({"DOUBLE", "STRING"})
    public Value.Type valueType;

    private Table[] t;
    private Value[] v;
    private Session s;
    private Writer w;
    private Timespec ts;

    private Value[] blobValues;
    private Value[] doubleValues;

    public WriterBenchmark() {
        for (int i = 0; i < this.columnCount; ++i) {
            this.v[i] = TestUtils.generateRandomValueByType(this.columnType);
        }

    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.s = TestUtils.createSession();
        this.ts = Timespec.now();

        this.v = new Value[this.columnCount];
        for (int i = 0; i < this.columnCount; ++i) {
            this.v[i] = TestUtils.generateRandomValueByType(this.columnType);
        }
    }

    @TearDown(Level.Trial)
    public void teardownTrial() throws Exception {
        this.v = null;
        this.s.close();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws Exception {
        this.s.purgeAll(300000);

        Column[] c = TestUtils.generateTableColumns(this.columnType, this.columnCount);

        this.t = new Table[this.tableCount];
        for (int i = 0; i < this.tableCount; ++i) {
            Table t = TestUtils.createTable(this.s, c);
            this.t[i] = t;
        }

        if (this.writerType.equals("writer")) {
            this.w = Tables.writer(this.s, this.t, this.pushMode);
        } else if (this.writerType.equals("pinnedWriter")) {
            this.w = Tables.pinnedWriter(this.s, this.t, this.pushMode);
        } else if (this.writerType.equals("expWriter")) {
            this.w = Tables.expWriter(this.s, this.t, this.pushMode);
        } else {
            throw new RuntimeException("Unrecognized writer type: " + this.writerType);
        }

        if (this.operationType.equals("FLUSH")) {
            this.appendRows();
        }
    }

    @TearDown(Level.Invocation)
    public void teardownInvocation() throws Exception {
        this.w.close();
        this.w = null;
        this.t = null;

        System.gc();
    }

    void appendFirstTableRows() throws Exception {
        for (int i = 0; i < this.rowCount; ++i) {
            this.w.append(0, this.ts, this.v);
        }
    }

    void appendManyTablesRows()  throws Exception {
        for (int i = 0; i < this.rowCount; ++i) {
            int randomTable = ThreadLocalRandom.current().nextInt(0, this.tableCount);
            int offset = randomTable * this.columnCount;

            this.w.append(offset, this.ts, this.v);
        }
    }

    void appendRows() throws Exception {
        if (this.tableSpread.equals("FIRST")) {
            this.appendFirstTableRows();
        } else if (this.tableSpread.equals("MANY")) {
            this.appendManyTablesRows();
        }

        if (this.writerType.equals("pinnedWriter")) {
            PinnedWriter pw = (PinnedWriter)this.w;
            pw.prepareFlush();
        } else if (this.writerType.equals("expWriter")) {
            ExpWriter pw = (ExpWriter)this.w;
            pw.prepareFlush();
        }
    }

    void flush() throws Exception {
        if (!this.operationType.equals("APPEND"))  {
            this.w.flush();
        }

    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Fork(0)
    @Warmup(iterations = 0)
    @Measurement(batchSize = -1, iterations = 5, time = 1, timeUnit = TimeUnit.MILLISECONDS)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void benchmark() throws Exception {
        if (!this.operationType.equals("FLUSH")) {
            this.appendRows();
        }

        if (!this.operationType.equals("APPEND")) {
            this.flush();
        }
    }
}
