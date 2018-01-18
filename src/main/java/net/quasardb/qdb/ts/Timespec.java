package net.quasardb.qdb.ts;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.Clock;

import net.quasardb.qdb.jni.*;

public class Timespec implements Serializable {
    private static Clock clock = new NanoClock();
    protected long sec;
    protected long nsec;

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

    public static Timespec now() {
        return new Timespec(Instant.now(Timespec.clock));
    }

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

    public Instant asInstant() {
        return Instant.ofEpochSecond(this.sec,
                                     this.nsec);
    }

    public LocalDateTime asLocalDateTime() {
        return LocalDateTime.ofInstant(this.asInstant(),
                                       ZoneId.systemDefault());
    }

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
