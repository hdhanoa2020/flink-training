package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.training.assignments.domain.Position;

import java.io.IOException;

public class PositionDeserializationSchema implements
        DeserializationSchema<Position> {

    // static ObjectMapper objectMapper = new ObjectMapper();
    static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public Position deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, Position.class);
    }

    @Override
    public boolean isEndOfStream(Position nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Position> getProducedType() {
        return TypeInformation.of(Position.class);
    }
}