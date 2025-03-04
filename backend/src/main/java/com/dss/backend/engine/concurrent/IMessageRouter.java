package com.dss.backend.engine.concurrent;

import java.util.Set;

public interface IMessageRouter {
    void registerNode(String nodeId, VirtualNode node);
    void messageSent(SimulationMessage message);
    Set<String> getRegisteredNodeIds();
}