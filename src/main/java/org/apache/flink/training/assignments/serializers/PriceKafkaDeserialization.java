package org.apache.flink.training.assignments.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.training.assignments.domain.Price;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PriceKafkaDeserialization implements KafkaDeserializationSchema<Price>
{
    private static final Logger LOG = LoggerFactory.getLogger(PriceKafkaDeserialization.class);
    static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public boolean isEndOfStream(Price nextElement) {
        return false;
    }

    @Override
    public Price deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        Price price = objectMapper.readValue(record.value(), Price.class);
        price.setTimestamp(record.timestamp());
        LOG.debug("price object: {} : {}",price.getCusip(), price.getPrice());
        return price;
    }

    @Override
    public TypeInformation<Price> getProducedType() {
        return TypeInformation.of(Price.class);
    }
}


