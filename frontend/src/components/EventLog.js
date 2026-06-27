// A scrolling list of events pushed live over the simulation's WebSocket topic.
export default function EventLog({ events }) {
  return (
    <div className="event-log">
      <h3>Event Log</h3>
      <ul>
        {events
          .slice()
          .reverse()
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
