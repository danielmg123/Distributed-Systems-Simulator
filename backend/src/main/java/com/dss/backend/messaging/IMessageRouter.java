package com.dss.backend.messaging;

import java.util.Set;

public interface IMessageRouter {
    void registerNode(String nodeId, VirtualNode node);
    void messageSent(SimulationMessage message);
    Set<String> getRegisteredNodeIds();
}