package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderKafkaDeserialization implements KafkaDeserializationSchema<Order>
{
    private static final Logger LOG = LoggerFactory.getLogger(OrderKafkaDeserialization.class);
    static ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public boolean isEndOfStream(Order nextElement) {
        return false;
    }
    @Override
    public Order deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        Order order = objectMapper.readValue(record.value(), Order.class);
        order.setTimestamp(record.timestamp());
        LOG.debug("order.getTimestamp(): {}",order.getTimestamp());
        return order;
    }
    @Override
    public TypeInformation<Order> getProducedType() {
        return TypeInformation.of(Order.class);
    }
}