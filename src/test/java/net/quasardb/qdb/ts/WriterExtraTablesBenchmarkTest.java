package net.quasardb.qdb.ts;

import java.util.*;
import java.time.*;
import java.lang.Exception;
import java.util.stream.Stream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

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
public class WriterExtraTablesBenchmarkTest {

    @Param({"10"})
    public int columnCount;

    @Param({"1000"})
    public int rowCount;

    @Param({"10000"})
    public int tableCount;

    @Param({"DOUBLE", "INT64", "TIMESTAMP"})
    public Value.Type valueType;

    @Param({"pinnedWriter", "writer"})
    public String writerType;

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
            this.w = Tables.writer(this.s, this.t);
        } else if (this.writerType.equals("pinnedWriter")) {
            this.w = Tables.pinnedWriter(this.s, this.t);
        } else {
            throw new RuntimeException("Unrecognized writer type: " + this.writerType);
        }
    }

    @TearDown(Level.Invocation)
    public void teardownInvocation() throws Exception {
        this.v = null;
        this.w.close();
        this.w = null;
        this.t = null;
    }


    @Test
    public void benchmark() throws Exception {
        // Junit entrypoint, junit -> jmh wrapper
        String[] argv = {};
        org.openjdk.jmh.Main.main(argv);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 5)
    @Measurement(batchSize = -1, iterations = 1, time = 1, timeUnit = TimeUnit.MILLISECONDS)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void appendBenchmark() throws Exception {

        System.out.println("first table name: " + this.t[0].getName());

        for (int i = 0; i < this.rowCount; ++i) {
            this.w.append(0, this.ts, this.v);
        }

        this.w.flush();
    }
}
