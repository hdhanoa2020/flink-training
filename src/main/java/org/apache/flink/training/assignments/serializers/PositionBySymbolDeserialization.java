
package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.training.assignments.domain.PositionBySymbol;

import java.io.IOException;

public class PositionBySymbolDeserialization implements
        DeserializationSchema<PositionBySymbol> {

    // static ObjectMapper objectMapper = new ObjectMapper();
    static ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public PositionBySymbol deserialize(byte[] message) throws IOException {
        return objectMapper.readValue(message, PositionBySymbol.class);
    }

    @Override
    public boolean isEndOfStream(PositionBySymbol nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PositionBySymbol> getProducedType() {
        return TypeInformation.of(PositionBySymbol.class);
    }
}
