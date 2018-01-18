package net.quasardb.qdb.ts;

import java.time.ZoneId;
import java.time.Instant;
import java.time.Clock;

public class NanoClock extends Clock
{
    private final Clock clock;
    private final long initialNanos;
    private final Instant initialInstant;

    public NanoClock()
    {
        this(java.time.Clock.systemUTC());
    }

    public NanoClock(final Clock clock)
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
    public NanoClock withZone(final ZoneId zone)
    {
        return new NanoClock(clock.withZone(zone));
    }

    private long getSystemNanos()
    {
        return System.nanoTime();
    }
}
