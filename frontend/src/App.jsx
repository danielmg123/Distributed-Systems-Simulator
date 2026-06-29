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
      <header className="app-header">
        <div className="app-header__brand">
          <span className="app-header__mark" aria-hidden="true" />
          <div>
            <h1 className="app-header__title">Distributed Systems Simulator</h1>
            <p className="app-header__subtitle">Paxos · Multi-Paxos · Raft</p>
          </div>
        </div>
        {simulationId && (
          <div className="app-header__meta">
            <span className="status-dot status-dot--live" aria-hidden="true" />
            <span className="app-header__state">Running</span>
            <code className="app-header__sim-id">{simulationId}</code>
          </div>
        )}
      </header>

      {error && (
        <p className="error" onClick={() => setError(null)}>
          {error} (click to dismiss)
        </p>
      )}

      {!simulationId ? (
        <SimulationSetup onSimulationStarted={setSimulationId} />
      ) : (
        <main className="dashboard">
          <section className="panel panel--topology">
            <div className="panel__head">
              <h2 className="panel__title">Topology</h2>
            </div>
            <TopologyGraph nodes={nodes} topology={topology} events={events} />
          </section>

          <section className="panel panel--nodes">
            <div className="panel__head">
              <h2 className="panel__title">Nodes</h2>
              <span className="panel__count">{nodes.length}</span>
            </div>
            <NodeGrid nodes={nodes} onFailNode={handleFailNode} onRecoverNode={handleRecoverNode} />
          </section>

          <div className="dashboard__row">
            <section className="panel">
              <div className="panel__head">
                <h2 className="panel__title">Controls</h2>
              </div>
              <Controls onPropose={handlePropose} onStop={handleStop} />
            </section>
            <section className="panel">
              <div className="panel__head">
                <h2 className="panel__title">Network conditions</h2>
              </div>
              <NetworkSliders onChange={handleNetworkConditionsChange} />
            </section>
            <section className="panel">
              <div className="panel__head">
                <h2 className="panel__title">Metrics</h2>
              </div>
              <MetricsPanel metrics={metrics} />
            </section>
          </div>

          <section className="panel panel--events">
            <div className="panel__head">
              <h2 className="panel__title">Event log</h2>
            </div>
            <EventLog events={events} />
          </section>
        </main>
      )}
    </div>
  );
}

export default App;
