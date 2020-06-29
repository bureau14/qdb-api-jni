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

    @Param({"5"})
    public int columnCount;

    @Param({"1000", "10000"})
    public int rowCount;

    @Param({"1000", "10000"})
    public int tableCount;

    @Param({"pinnedWriter", "writer"})
    public String writerType;

    @Param({"NORMAL", "ASYNC"})
    public Writer.PushMode pushMode;

    @Param({"DOUBLE", "BLOB"})
    public Value.Type valueType;

    private Table[] t;
    private Value[] v;
    private Session s;
    private Writer w;
    private Timespec ts;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        this.s = TestUtils.createSession();
    }

    @TearDown(Level.Trial)
    public void teardownTrial() throws Exception {
        this.s.close();
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws Exception {
        this.ts = Timespec.now();

        this.v = new Value[this.columnCount];
        for (int i = 0; i < this.columnCount; ++i) {
            this.v[i] = TestUtils.generateRandomValueByType(this.valueType);
        }

        Column[] c = TestUtils.generateTableColumns(this.valueType, this.columnCount);

        this.t = new Table[this.tableCount];
        for (int i = 0; i < this.tableCount; ++i) {
            Table t = TestUtils.createTable(this.s, c);
            this.t[i] = t;
        }

        if (this.writerType.equals("writer")) {
            this.w = Tables.writer(this.s, this.t, this.pushMode);
        } else if (this.writerType.equals("pinnedWriter")) {
            this.w = Tables.pinnedWriter(this.s, this.t, this.pushMode);
        } else {
            throw new RuntimeException("Unrecognized writer type: " + this.writerType);
        }
    }

    @TearDown(Level.Invocation)
    public void teardownInvocation() throws Exception {
        this.v = null;
        this.w.close();
        this.w = null;

        for (Table t : this.t) {
            Table.remove(this.s, t);
        }

        this.t = null;
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 5)
    @Measurement(batchSize = -1, iterations = 3, time = 1, timeUnit = TimeUnit.MILLISECONDS)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void appendFirstTableBenchmark() throws Exception {
        for (int i = 0; i < this.rowCount; ++i) {
            this.w.append(0, this.ts, this.v);
        }

        this.w.flush();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 5)
    @Measurement(batchSize = -1, iterations = 3, time = 1, timeUnit = TimeUnit.MILLISECONDS)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void appendManyTablesBenchmark() throws Exception {

        for (int i = 0; i < this.rowCount; ++i) {
            int randomTable = ThreadLocalRandom.current().nextInt(0, this.tableCount);
            int offset = randomTable * this.columnCount;

            this.w.append(offset, this.ts, this.v);
        }

        this.w.flush();
    }
}
