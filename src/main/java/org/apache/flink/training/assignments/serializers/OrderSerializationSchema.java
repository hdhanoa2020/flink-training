package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.training.assignments.domain.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderSerializationSchema
        implements SerializationSchema<Order> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(Order element) {
        byte[] b = null;
      try{
           b = objectMapper.writeValueAsString(element).getBytes();
      }catch(Exception e){

      }
        return b;
    }
}