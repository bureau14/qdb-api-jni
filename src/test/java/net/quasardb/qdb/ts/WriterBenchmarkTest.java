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
public class WriterBenchmarkTest {

    @Param({"25"})
    public int columnCount;

    @Param({"10000"})
    public int rowCount;

    @Param({"DOUBLE", "BLOB"})
    public Value.Type valueType;

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
        Table t = TestUtils.createTable(this.s, c);
        this.w = Table.writer(this.s, t);
    }

    @TearDown(Level.Invocation)
    public void teardownInvocation() throws Exception {
        this.v = null;
    }


    @Test
    public void benchmark() throws Exception {
        // Junit entrypoint, junit -> jmh wrapper
        String[] argv = {};
        org.openjdk.jmh.Main.main(argv);
    }


    /**
     *
     * Baseline:
     *
Benchmark                 (columnCount)  (rowCount)  (valueType)  Mode  Cnt    Score    Error  Units
WriterBenchmarkTest.test             25       10000       DOUBLE  avgt   10  758.726 ± 13.983  ms/op
WriterBenchmarkTest.test             25       10000         BLOB  avgt   10  834.138 ± 24.322  ms/op

     */

    /**
     * After optimizations:
     *

Benchmark                 (columnCount)  (rowCount)  (valueType)  Mode  Cnt    Score   Error  Units
WriterBenchmarkTest.test             25       10000       DOUBLE  avgt   15  112.080 ± 3.283  ms/op
WriterBenchmarkTest.test             25       10000         BLOB  avgt   15  161.123 ± 4.686  ms/op

     */

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @Warmup(batchSize = -1, iterations = 1, time = 10, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(batchSize = -1, iterations = 3, time = 10, timeUnit = TimeUnit.MILLISECONDS)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void test() throws Exception {

        for (int i = 0; i < this.rowCount; ++i) {
            this.w.append(0, this.ts, this.v);
        }
    }



}
