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

  // The backend returns 404/409 when a simulation is no longer running (e.g. it was
  // stopped, or the backend restarted and lost the in-memory run). Treat that as the end
  // of this run: show a clear message and drop back to the setup screen instead of
  // leaving a stale dashboard that silently does nothing. Other (transient) failures just
  // surface their message and leave the dashboard in place.
  const handleApiError = useCallback((err) => {
    if (err && (err.status === 404 || err.status === 409)) {
      setError("This simulation is no longer running. Start a new one.");
      setSimulationId(null);
    } else {
      setError(err.message);
    }
  }, []);

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
        .catch((err) => !cancelled && handleApiError(err));
    };
    poll();
    const interval = setInterval(poll, NODE_POLL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [simulationId, handleApiError]);

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
      .catch((err) => !cancelled && handleApiError(err));
    return () => {
      cancelled = true;
    };
  }, [simulationId, handleApiError]);

  const handleFailNode = useCallback(
    (nodeId) => api.failNode(simulationId, nodeId).catch(handleApiError),
    [simulationId, handleApiError]
  );
  const handleRecoverNode = useCallback(
    (nodeId) => api.recoverNode(simulationId, nodeId).catch(handleApiError),
    [simulationId, handleApiError]
  );
  const handlePropose = useCallback(
    (value) => api.propose(simulationId, value).catch(handleApiError),
    [simulationId, handleApiError]
  );
  const handleStop = useCallback(() => {
    api
      .stopSimulation(simulationId)
      .then(() => setSimulationId(null))
      .catch((err) => setError(err.message));
  }, [simulationId]);
  const handleNetworkConditionsChange = useCallback(
    (conditions) =>
      simulationId && api.updateNetworkConditions(simulationId, conditions).catch(handleApiError),
    [simulationId, handleApiError]
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
