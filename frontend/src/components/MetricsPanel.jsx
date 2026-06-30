// Displays the latest MetricsSnapshot pushed over the simulation's WebSocket topic as a
// grid of stat tiles. Numbers are mono and thousands-separated; commits read green (the
// happy path) and dropped messages turn red once any are lost.
export default function MetricsPanel({ metrics }) {
  if (!metrics) {
    return <p className="metrics-panel__empty">Waiting for metrics…</p>;
  }

  const fmt = (n) => (typeof n === "number" ? n.toLocaleString() : n);
  const dropped = metrics.totalDroppedMessages;

  return (
    <div className="metrics-panel">
      <div className="metric">
        <span className="metric__label">Messages</span>
        <span className="metric__value">{fmt(metrics.totalMessages)}</span>
      </div>
      <div className="metric">
        <span className="metric__label">Dropped</span>
        <span className={`metric__value${dropped > 0 ? " metric__value--alert" : ""}`}>
          {fmt(dropped)}
        </span>
      </div>
      <div className="metric">
        <span className="metric__label">Proposals</span>
        <span className="metric__value">{fmt(metrics.totalProposals)}</span>
      </div>
      <div className="metric">
        <span className="metric__label">Commits</span>
        <span className="metric__value metric__value--good">{fmt(metrics.totalCommits)}</span>
      </div>
    </div>
  );
}
