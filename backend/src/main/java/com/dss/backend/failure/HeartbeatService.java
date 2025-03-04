package com.dss.backend.failure;

import com.dss.backend.engine.Scheduler;

import java.util.concurrent.ScheduledExecutorService;

public interface HeartbeatService {
    void start(Scheduler scheduler);
    void stop();
}