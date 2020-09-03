package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderCusipMap implements MapFunction<Order, Position> {

    private static final Logger LOG = LoggerFactory.getLogger(OrderCusipMap.class);

    @Override
    public Position map(Order o) throws Exception {
        String cusip = o.getCusip();
        int  quantity = (o.getBuySell() == (BuySell.BUY) ? o.getQuantity() : -o.getQuantity());
        Position position = new Position(o.getCusip(),o.getBuySell(),null,null,quantity,o.getOrderId()); //Position(order.getCusip(), adjustQuantity(order));
        position.setTimestamp(o.getTimestamp());
        return position;
    }

}
