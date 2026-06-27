import { useEffect, useRef, useState } from "react";
import { Client } from "@stomp/stompjs";
import SockJS from "sockjs-client";

// Subscribes to a running simulation's live event and metrics topics over STOMP
// (backed by SockJS, matching the backend's WebSocketConfig). Reconnects
// automatically if the connection drops. Returns the latest metrics snapshot and a
// running list of events for the given simulationId, or null/[] if simulationId is
// null (no simulation selected/running).
export function useSimulationSocket(simulationId) {
  const [metrics, setMetrics] = useState(null);
  const [events, setEvents] = useState([]);
  const clientRef = useRef(null);

  useEffect(() => {
    setMetrics(null);
    setEvents([]);
    if (!simulationId) {
      return;
    }

    const client = new Client({
      webSocketFactory: () => new SockJS("/ws"),
      reconnectDelay: 2000,
    });

    client.onConnect = () => {
      client.subscribe(`/topic/simulation/${simulationId}/metrics`, (message) => {
        setMetrics(JSON.parse(message.body));
      });
      client.subscribe(`/topic/simulation/${simulationId}/events`, (message) => {
        const event = JSON.parse(message.body);
        setEvents((prev) => [...prev, event].slice(-200)); // cap so the log doesn't grow forever
      });
    };

    client.activate();
    clientRef.current = client;

    return () => {
      client.deactivate();
      clientRef.current = null;
    };
  }, [simulationId]);

  return { metrics, events };
}
