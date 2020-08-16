package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.training.assignments.domain.Position;

public class AccountAllocationSerializationSchema
        implements SerializationSchema<Position> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Position element) {
        byte[] b = null;
        try{
            b = objectMapper.writeValueAsString(element).getBytes();
        }catch(Exception e){

        }
        return b;
    }
}