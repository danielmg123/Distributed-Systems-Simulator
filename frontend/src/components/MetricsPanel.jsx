// Displays the latest MetricsSnapshot pushed over the simulation's WebSocket topic.
export default function MetricsPanel({ metrics }) {
  if (!metrics) {
    return <p className="metrics-panel__empty">Waiting for metrics…</p>;
  }
  return (
    <div className="metrics-panel">
      <dl>
        <dt>Messages</dt>
        <dd>{metrics.totalMessages}</dd>
        <dt>Dropped</dt>
        <dd>{metrics.totalDroppedMessages}</dd>
        <dt>Proposals</dt>
        <dd>{metrics.totalProposals}</dd>
        <dt>Commits</dt>
        <dd>{metrics.totalCommits}</dd>
      </dl>
    </div>
  );
}
