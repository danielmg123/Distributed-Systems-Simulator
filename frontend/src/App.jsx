import { useCallback, useEffect, useState } from "react";
import { api } from "./api";
import { useSimulationSocket } from "./useSimulationSocket";
import SimulationSetup from "./components/SimulationSetup";
import NodeGrid from "./components/NodeGrid";
import TopologyGraph from "./components/TopologyGraph";
import EventLog from "./components/EventLog";
import Controls from "./components/Controls";
import NetworkSliders from "./components/NetworkSliders";
import MetricsPanel from "./components/MetricsPanel";
import "./App.css";

const NODE_POLL_MS = 1000;

function App() {
  const [simulationId, setSimulationId] = useState(null);
  const [nodes, setNodes] = useState([]);
  const [topology, setTopology] = useState({});
  const [error, setError] = useState(null);
  const { metrics, events } = useSimulationSocket(simulationId);

  useEffect(() => {
    if (!simulationId) {
      setNodes([]);
      return;
    }
    let cancelled = false;
    const poll = () => {
      api
        .getNodeStatuses(simulationId)
        .then((result) => {
          if (!cancelled) {
            setNodes(result);
          }
        })
        .catch((err) => !cancelled && setError(err.message));
    };
    poll();
    const interval = setInterval(poll, NODE_POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [simulationId]);

  // The topology adjacency map is fixed for a run, so fetch it once. Node colors update
  // live via the node-status poll above.
  useEffect(() => {
    if (!simulationId) {
      setTopology({});
      return;
    }
    let cancelled = false;
    api
      .getTopology(simulationId)
      .then((result) => !cancelled && setTopology(result || {}))
      .catch((err) => !cancelled && setError(err.message));
    return () => {
      cancelled = true;
    };
  }, [simulationId]);

  const handleFailNode = useCallback(
    (nodeId) => api.failNode(simulationId, nodeId).catch((err) => setError(err.message)),
    [simulationId]
  );
  const handleRecoverNode = useCallback(
    (nodeId) => api.recoverNode(simulationId, nodeId).catch((err) => setError(err.message)),
    [simulationId]
  );
  const handlePropose = useCallback(
    (value) => api.propose(simulationId, value).catch((err) => setError(err.message)),
    [simulationId]
  );
  const handleStop = useCallback(() => {
    api
      .stopSimulation(simulationId)
      .then(() => setSimulationId(null))
      .catch((err) => setError(err.message));
  }, [simulationId]);
  const handleNetworkConditionsChange = useCallback(
    (conditions) =>
      simulationId && api.updateNetworkConditions(simulationId, conditions).catch((err) => setError(err.message)),
    [simulationId]
  );

  return (
    <div className="app">
      <h1>Distributed Systems Simulator</h1>
      {error && (
        <p className="error" onClick={() => setError(null)}>
          {error} (click to dismiss)
        </p>
      )}

      {!simulationId ? (
        <SimulationSetup onSimulationStarted={setSimulationId} />
      ) : (
        <div className="dashboard">
          <p className="dashboard__sim-id">
            Simulation: <code>{simulationId}</code>
          </p>
          <NodeGrid nodes={nodes} onFailNode={handleFailNode} onRecoverNode={handleRecoverNode} />
          <TopologyGraph nodes={nodes} topology={topology} />
          <div className="dashboard__row">
            <Controls onPropose={handlePropose} onStop={handleStop} />
            <NetworkSliders onChange={handleNetworkConditionsChange} />
            <MetricsPanel metrics={metrics} />
          </div>
          <EventLog events={events} />
        </div>
      )}
    </div>
  );
}

export default App;
