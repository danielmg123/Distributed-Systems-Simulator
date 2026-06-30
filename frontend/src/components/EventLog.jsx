// A scrolling list of events pushed live over the simulation's WebSocket topic.
// Sorted newest-first by timestamp rather than arrival order: near-simultaneous events
// can be delivered out of order over the WebSocket, which otherwise made a commit show
// up above its own proposal. Timestamps are fixed-format ISO strings, so a plain
// descending string compare is chronological.
//
// Each row is tinted by event category so the log is scannable: commits and recoveries
// read green, failures red, elections cyan, and routine proposals/sim events stay muted.

const TYPE_COLOR = {
  LEADER_ELECTED: "var(--status-leader)",
  VALUE_COMMITTED: "var(--status-active)",
  NODE_RECOVERED: "var(--status-active)",
  NODE_FAILED: "var(--status-failed)",
  VALUE_PROPOSED: "var(--text-secondary)",
  SIMULATION_EVENT: "var(--text-muted)",
  SIMULATION_STARTED: "var(--text-muted)",
};

function colorForType(type) {
  return TYPE_COLOR[type] || "var(--text-muted)";
}

// Show just the time of day; the full ISO timestamp stays available on hover.
function formatTime(ts) {
  if (!ts) return "";
  const time = ts.split("T")[1];
  return time ? time.slice(0, 8) : ts;
}

export default function EventLog({ events }) {
  if (!events || events.length === 0) {
    return <p className="event-log__empty">No events yet.</p>;
  }

  return (
    <div className="event-log">
      <ul>
        {events
          .slice()
          .sort((a, b) => {
            const ta = a.timestamp || "";
            const tb = b.timestamp || "";
            return ta < tb ? 1 : ta > tb ? -1 : 0;
          })
          .map((event, i) => (
            <li key={i} style={{ "--ev-color": colorForType(event.type) }}>
              <span className="event-log__type">{event.type}</span>
              <span className="event-log__details">{event.details}</span>
              <time className="event-log__time" dateTime={event.timestamp} title={event.timestamp}>
                {formatTime(event.timestamp)}
              </time>
            </li>
          ))}
      </ul>
    </div>
  );
}
