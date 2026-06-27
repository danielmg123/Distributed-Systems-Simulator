package com.dss.backend.engine;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The <strong>Scheduler</strong> interface defines the abstraction for scheduling tasks
 * in the simulation. By using a dedicated Scheduler rather than a plain {@link java.util.concurrent.Executor},
 * the simulator can:
 * <ul>
 *   <li>Schedule periodic tasks, such as heartbeats or timeouts, in a uniform way.</li>
 *   <li>Start and stop all simulation-related tasks cleanly when the simulation begins or ends.</li>
 *   <li>swap out the implementation (e.g. a mock or virtual time scheduler) for testing.</li>
 * </ul>
 * <p>
 * Methods include scheduling both one-time and repeating tasks, plus operations for
 * shutdown and awaiting termination.
 */
public interface Scheduler {

    /**
     * Schedules a single action to occur after the given delay.
     *
     * @param command the task to run
     * @param delay   the delay before execution
     * @param unit    the time unit of the delay
     * @return a {@link ScheduledFuture} that can be used to cancel or check execution
     */
    ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

    /**
     * Schedules a recurring action. The task will first execute after the specified
     * initial delay, and then repeatedly execute with the given period between runs.
     *
     * @param command       the task to run periodically
     * @param initialDelay  time to delay first execution
     * @param period        time between subsequent executions
     * @param unit          the time unit for <code>initialDelay</code> and <code>period</code>
     * @return a {@link ScheduledFuture} for the scheduled task
     */
    ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

    /**
     * Initiates a graceful shutdown of the scheduler, stopping acceptance of new tasks.
     */
    void shutdown();

    /**
     * @return {@code true} if this scheduler has been shut down and will reject new tasks.
     */
    boolean isShutdown();

    /**
     * Blocks until all tasks have completed execution after a shutdown request,
     * or the timeout occurs.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout
     * @return true if the scheduler terminated and false if the timeout elapsed
     * @throws InterruptedException if interrupted while waiting
     */
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Attempts to immediately stop all tasks and returns a list of tasks that never commenced.
     *
     * @return list of tasks that never started
     */
    List<Runnable> shutdownNow();
}