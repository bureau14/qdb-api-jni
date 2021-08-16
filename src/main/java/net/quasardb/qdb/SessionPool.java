package net.quasardb.qdb;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Session / connection pool. This class is thread-safe.
 *
 * Simple, fixed-size pool where sessions are pre-allocated in the constructor.
 */
public class SessionPool {

    private static final Logger logger = LoggerFactory.getLogger(SessionPool.class);

    private SessionFactory factory;
    private BlockingDeque<Session> stack;

    public SessionPool(SessionFactory factory, int size) {
        this.factory             = factory;
        this.stack               = preallocate(factory, size);
    }

    private BlockingDeque<Session> preallocate(SessionFactory factory, int size) {
        BlockingDeque<Session> stack = new LinkedBlockingDeque<Session> (size);
        logger.info("Preallocating {} sessions", size);

        ExecutorService executor = Executors.newFixedThreadPool(size);

        List<CompletableFuture<Session>> jobs = new ArrayList<>();

        for (int i = 0; i < size; ++i) {
            logger.info("Preallocating session {}", i);
            CompletableFuture<Session> future = new CompletableFuture<Session>();

            executor.submit(() -> {
                    try {
                        future.complete(factory.newSession());
                        return null;
                    } catch (Throwable e) {
                        future.completeExceptionally(e);
                        return null;
                    }
                });

            jobs.add(future);
        }

        try {
            for (CompletableFuture<Session> job : jobs) {
                logger.info("Waiting for session allocation...");
                stack.add(job.get());
            }
        } catch (Exception e) {
            logger.error("Error while allocating sessions", e);
            throw new RuntimeException("Error while allocating sessions");
        }

        logger.info("Preallocated all sessions");
        return stack;
    }

    public Session acquire() throws InterruptedException, IOException {
        return this.stack.take();
    }

    public void release(Session s) throws InterruptedException {
        this.stack.put(s);
    }

    public int size() {
        return this.stack.size();
    }

    public void close() throws IOException {
        for (Session s : this.stack) {
            s.close();
        }
    }
}
