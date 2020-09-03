package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.util.Collector;

public class OrderFlatMap implements FlatMapFunction<Order, Position> {


    @Override
    public void flatMap(Order o, Collector<Position> collector) throws Exception {
    String cusip = o.getCusip();
    BuySell buySell = o.getBuySell();
    for (Allocation a :o.getAllocations()){
        Position position = new Position(cusip,buySell,a.getAccount(), a.getSubAccount(),a.getQuantity(),o.getOrderId());
        position.setTimestamp(o.getTimestamp());
        position.setPositionKey(cusip+ "-"+a.getAccount()+"-"+a.getSubAccount());
        collector.collect(position);
    }
  }
}
