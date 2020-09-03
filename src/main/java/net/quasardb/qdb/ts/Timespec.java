package net.quasardb.qdb.ts;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.Instant;
import java.time.Clock;

import net.quasardb.qdb.jni.*;
import net.quasardb.qdb.exception.InvalidArgumentException;

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
     * Construct a new Timespec null value. Constants align with what is used by QuasarDB
     * in the backend.
     */
    public Timespec(){
        this.sec = Constants.minTime;
        this.nsec = Constants.minTime;
    }

    /**
     * Construct a new timespec from milliseconds.
     */
    public Timespec(long msec) {
        this.sec = msec / 1000;
        this.nsec = (msec % 1000) * 1000000;
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

    public boolean isBefore(Timespec rhs) {
        if (this.sec < rhs.sec) {
            return true;
        } else if (this.sec == rhs.sec &&
            this.nsec < rhs.nsec) {
            return true;
        }

        return false;
    }

    public boolean isEmpty() {
        return this.sec == -1 && this.nsec == -1;
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
     * Returns copy of this instance with the specified duration in seconds deducted.
     */
    public Timespec minusSeconds(long secondsToDeduct) {
        return new Timespec(this.sec - secondsToDeduct,
                            this.nsec);
    }

    /**
     * Returns copy of this instance with the specified duration in nanoseconds deducted.
     */
    public Timespec minusNanos(long nanosToDeduct) {
        return new Timespec(this.sec,
                            this.nsec - nanosToDeduct);
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
        return "Timespec (" + this.asInstant().toString() + ")";
    }

    /**
     * Converts this timespec to the number of milliseconds from the epoch of 1970-01-01
     */
    public long toEpochMillis() {
        return (long)(this.sec * 1000) + (long)(this.nsec / 1000000);
    }

    /**
     * Converts this timespec to the number of nanoseconds from the epoch of 1970-01-01
     */
    public long toEpochNanos() {
        return (this.sec * 1000000000) + this.nsec;
    }


    /**
     * Returns the smallest timespec between the two, that is, the time that is pointing
     * towards the earliest point in time.
     */
    public static Timespec min(Timespec lhs, Timespec rhs) {
        if (lhs.sec == -1 && rhs.sec != -1) {
            return rhs;
        }
        if (rhs.sec == -1 && lhs.sec != -1) {
            return lhs;
        }

        if (rhs.sec == -1 && lhs.sec == -1) {
            throw new InvalidArgumentException("Both time ranges are null.");
        }

        if (lhs.sec < rhs.sec) {
            return lhs;
        }

        if (rhs.sec < lhs.sec) {
            return rhs;
        }

        // lhs.sec == rhs.sec
        if (lhs.nsec < rhs.nsec) {
            return lhs;
        }

        return rhs;
    }

    /**
     * Returns the smallest timespec between the two, that is, the time that is pointing
     * towards the latest point in time.
     */
    public static Timespec max(Timespec lhs, Timespec rhs) {
        if (lhs.sec == -1 && rhs.sec != -1) {
            return rhs;
        }
        if (rhs.sec == -1 && lhs.sec != -1) {
            return lhs;
        }

        if (rhs.sec == -1 && lhs.sec == -1) {
            throw new InvalidArgumentException("Both time ranges are null.");
        }

        if (lhs.sec > rhs.sec) {
            return lhs;
        }
        if (rhs.sec > lhs.sec) {
            return rhs;
        }

        // lhs.sec == rhs.sec
        if (lhs.nsec > rhs.nsec) {
            return lhs;
        }

        return rhs;
    }

}
