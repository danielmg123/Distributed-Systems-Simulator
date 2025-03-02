package com.dss.backend.algorithm.failure;

import java.util.concurrent.ScheduledExecutorService;

public interface HeartbeatService {
    void start(ScheduledExecutorService scheduler);
    void stop();
}
