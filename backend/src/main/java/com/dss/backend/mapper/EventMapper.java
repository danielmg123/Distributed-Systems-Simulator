package com.dss.backend.mapper;

import com.dss.backend.dto.EventDTO;
import com.dss.backend.model.Event;
import org.mapstruct.Mapper;

/**
 * {@code EventMapper} transforms between a domain {@link Event} and its Data Transfer Object,
 * {@link EventDTO}.
 *
 * <p>The generated implementation converts fields like
 * <code>type, details, timestamp</code> automatically. If fields differ in name or need
 * custom logic, we can annotate methods to define the mapping explicitly.</p>
 *
 * <p><strong>usage:</strong></p>
 * <ul>
 *   <li>Mapping an {@code Event} from the database into {@link EventDTO} for
 *       REST responses or UI display.</li>
 *   <li>Mapping an incoming DTO from the REST layer to a domain {@code Event} to store
 *       or process further.</li>
 * </ul>
 */
@Mapper(componentModel = "spring")
public interface EventMapper {

    /**
     * Converts a domain {@link Event} to its corresponding {@link EventDTO}.
     *
     * @param event the event domain object
     * @return the Data Transfer Object version
     */
    EventDTO eventToEventDTO(Event event);
}
