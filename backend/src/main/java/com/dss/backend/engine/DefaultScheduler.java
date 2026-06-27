package com.dss.backend.engine;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * <strong>DefaultScheduler</strong> is a concrete implementation of the {@link Scheduler} interface,
 * wrapping a {@link ScheduledExecutorService} to schedule tasks in the simulator.
 * <p>
 * Rather than using a plain thread pool or executor, the scheduler abstraction
 * allows the simulator to:
 * <ul>
 *   <li>Consistently manage scheduled tasks (e.g. heartbeats, timeouts) in a single place.</li>
 *   <li>Control or mock scheduling behavior if needed for testing or time manipulation.</li>
 *   <li>Shut down simulation-related tasks gracefully when the simulation ends.</li>
 * </ul>
 * <p>
 * The <code>DefaultScheduler</code> delegates to a real {@link ScheduledExecutorService}
 * for actual scheduling.
 */
public class DefaultScheduler implements Scheduler {
    private final ScheduledExecutorService executorService;

    /**
     * Creates a new {@code DefaultScheduler} with the underlying
     * {@link ScheduledExecutorService} to handle task scheduling.
     *
     * @param executorService the thread pool for scheduling and executing tasks
     */
    public DefaultScheduler(ScheduledExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * Schedules a one-time action to occur after the specified delay.
     *
     * @param command the task to execute
     * @param delay   how long to wait before executing the task
     * @param unit    the time unit of the delay
     * @return a {@link ScheduledFuture} representing pending completion of the task
     */
    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return executorService.schedule(command, delay, unit);
    }

    /**
     * Schedules a periodic action, starting after the initial delay
     * and repeating at the given period.
     *
     * @param command       the task to execute
     * @param initialDelay  time to delay first execution
     * @param period        the period between successive executions
     * @param unit          the time unit of the initialDelay and period parameters
     * @return a {@link ScheduledFuture} representing the scheduled task
     */
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        return executorService.scheduleAtFixedRate(command, initialDelay, period, unit);
    }

    /**
     * Attempts to stop all actively executing tasks and halts the processing
     * of waiting tasks.
     */
    @Override
    public void shutdown() {
        executorService.shutdown();
    }

    /**
     * @return {@code true} if the underlying executor has been shut down.
     */
    @Override
    public boolean isShutdown() {
        return executorService.isShutdown();
    }

    /**
     * Blocks until all tasks have completed execution after a shutdown request,
     * or the timeout occurs, or the current thread is interrupted, whichever happens first.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return true if this executor terminated and false if the timeout elapsed before termination
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executorService.awaitTermination(timeout, unit);
    }

    /**
     * Attempts to stop all actively executing tasks, halts the processing of waiting tasks,
     * and returns a list of the tasks that were awaiting execution.
     *
     * @return the list of tasks that never commenced execution
     */
    @Override
    public List<Runnable> shutdownNow() {
        return executorService.shutdownNow();
    }
}