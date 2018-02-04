package net.quasardb.qdb.ts;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.Clock;

import net.quasardb.qdb.jni.*;

/**
 * Nanosecond precision time specification for QuasarDB. Allows construction from
 * multiple different clock sources, and interaction with the QuasarDB backend.
 *
 * @see Value
 * @see Row
 */
public class Timespec implements Serializable {
    private static Clock clock = new NanoClock();
    protected long sec;
    protected long nsec;

    /**
     * Construct a new Timespec without any time. Should typically not be used, as
     * these timespec values will be rejected by the QuasarDB backend.
     */
    public Timespec(){
        this.sec = -1;
        this.nsec = -1;
    }

    public Timespec(long sec, long nsec){
        this.sec = sec;
        this.nsec = nsec;
    }

    public Timespec (LocalDateTime value) {
        this(value.atZone(ZoneId.systemDefault()).toEpochSecond(),
             value.getNano());
    }

    public Timespec (Timestamp value) {
        this(value.toLocalDateTime());
    }

    public Timespec (Instant value) {
        this(value.getEpochSecond(),
             value.getNano());
    }

    public long getSec() {
        return this.sec;
    }

    public long getNano() {
        return this.nsec;
    }

    /**
     * Construct a new Timespec based on a {@link NanoClock} that provides nanosecond
     * precision.
     *
     * @see NanoClock
     */
    public static Timespec now() {
        return new Timespec(Instant.now(Timespec.clock));
    }

    /**
     * Construct a new Timespec using your own custom Clock.
     */
    public static Timespec now(Clock clock) {
        return new Timespec(Instant.now(clock));
    }

    /**
     * Returns copy of this instance with the specified duration in seconds added.
     */
    public Timespec plusSeconds(long secondsToAdd) {
        return new Timespec(this.sec + secondsToAdd,
                            this.nsec);
    }

    /**
     * Returns copy of this instance with the specified duration in nanoseconds added.
     */
    public Timespec plusNanos(long nanosToAdd) {
        return new Timespec(this.sec,
                            this.nsec + nanosToAdd);
    }

    /**
     * Converts this Timespec into an {@link Instant}.
     */
    public Instant asInstant() {
        return Instant.ofEpochSecond(this.sec,
                                     this.nsec);
    }

    /**
     * Converts this Timespec into an {@link LocalDateTime}.
     */
    public LocalDateTime asLocalDateTime() {
        return LocalDateTime.ofInstant(this.asInstant(),
                                       ZoneId.systemDefault());
    }

    /**
     * Converts this Timespec into an sql {@link Timestamp}.
     */
    public Timestamp asTimestamp() {
        return Timestamp.valueOf(this.asLocalDateTime());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Timespec)) return false;
        Timespec rhs = (Timespec)obj;

        return rhs.getSec() == this.sec && rhs.getNano() == this.nsec;
    }

    public String toString() {
        return "Timespec (sec: " + this.sec + ", nsec: " + this.nsec + ")";
    }
}
