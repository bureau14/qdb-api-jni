package net.quasardb.qdb;

import java.time.ZoneId;
import java.time.Instant;
import java.time.Clock;

public class QdbClock extends Clock
{
    private final Clock clock;
    private final long initialNanos;
    private final Instant initialInstant;

    public QdbClock()
    {
        this(Clock.systemUTC());
    }

    public QdbClock(final Clock clock)
    {
        this.clock = clock;
        initialInstant = clock.instant();
        initialNanos = getSystemNanos();
    }

    @Override
    public ZoneId getZone()
    {
        return clock.getZone();
    }

    @Override
    public Instant instant()
    {
        return initialInstant.plusNanos(getSystemNanos() - initialNanos);
    }

    @Override
    public Clock withZone(final ZoneId zone)
    {
        return new QdbClock(clock.withZone(zone));
    }

    private long getSystemNanos()
    {
        return System.nanoTime();
    }
}
