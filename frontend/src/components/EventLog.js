// A scrolling list of events pushed live over the simulation's WebSocket topic.
// Sorted newest-first by timestamp rather than arrival order: near-simultaneous events
// can be delivered out of order over the WebSocket, which otherwise made a commit show
// up above its own proposal. Timestamps are fixed-format ISO strings, so a plain
// descending string compare is chronological.
export default function EventLog({ events }) {
  return (
    <div className="event-log">
      <h3>Event Log</h3>
      <ul>
        {events
          .slice()
          .sort((a, b) => {
            const ta = a.timestamp || "";
            const tb = b.timestamp || "";
            return ta < tb ? 1 : ta > tb ? -1 : 0;
          })
          .map((event, i) => (
            <li key={i}>
              <span className="event-log__type">{event.type}</span>
              <span className="event-log__details">{event.details}</span>
              <span className="event-log__time">{event.timestamp}</span>
            </li>
          ))}
      </ul>
    </div>
  );
}
