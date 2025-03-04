package com.dss.backend.engine;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public interface Scheduler {
    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);
    ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);
    void shutdown();
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;
    List<Runnable> shutdownNow();
}