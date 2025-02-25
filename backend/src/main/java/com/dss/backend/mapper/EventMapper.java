package com.dss.backend.mapper;

import com.dss.backend.dto.EventDTO;
import com.dss.backend.model.Event;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface EventMapper {
    EventDTO eventToEventDTO(Event event);
}