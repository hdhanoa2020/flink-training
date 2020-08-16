package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;

public class PositionsByCusipFlatMap implements FlatMapFunction<Order, Position> {

    @Override
    public void flatMap(Order o, Collector<Position> collector) throws Exception {
        for (Allocation a :o.getAllocations()){
            Position position = new Position(o.getCusip(),o.getBuySell(),a.getAccount(), a.getSubAccount(),a.getQuantity(),o.getOrderId());
            collector.collect(position);
        }
    }
}

