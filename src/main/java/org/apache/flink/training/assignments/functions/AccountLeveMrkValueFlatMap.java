package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.Price;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class AccountLeveMrkValueFlatMap extends RichCoFlatMapFunction<Price, Position, Position> {

    private static final Logger LOG = LoggerFactory.getLogger(AccountLeveMrkValueFlatMap.class);

    private ValueState<Price> priceState;
    private ValueState<Position> positionState;

    @Override
    public void flatMap1(Price price, Collector<Position> out) throws Exception {
        Position position = positionState.value();
        if (position != null) {
            positionState.clear();
            position.setPrice(price.getPrice());
            BigDecimal mrk = price.getPrice().multiply(BigDecimal.valueOf(position.getQuantity()));
            position.setMarketValue(mrk);
            out.collect(position);
        } else {
            priceState.update(price);
        }
    }

    @Override
    public void flatMap2(Position position, Collector<Position> out) throws Exception {
        Price price = priceState.value();
        if (price != null) {
            positionState.clear();
            position.setPrice(price.getPrice());
            BigDecimal mrk = price.getPrice().multiply(BigDecimal.valueOf(position.getQuantity()));
            position.setMarketValue(mrk);
            out.collect(position);
        } else {
            positionState.update(position);
        }
    }

    @Override
    public void open(Configuration config) {
        priceState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved priceState", Price.class));
        positionState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved positionState", Position.class));
    }
}
