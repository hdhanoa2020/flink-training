package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OrderCusipSumWindow extends ProcessWindowFunction<Order, Position, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(PostionsByCusip.class);

    @Override
    public void process(String cusip, Context context, Iterable<Order> iterable, Collector<Position> collector) throws Exception {
        int sum =0;
        String orderId="";
        for(Order a : iterable){
            // LOG.info("inside cusip maps..{}",a);
            sum = sum +  (a.getBuySell() == (BuySell.BUY) ? a.getQuantity() : -a.getQuantity());
            orderId=a.getOrderId(); //get the last one to calculate latency (matrix)
        }
        Position positionBySymbol = new Position(cusip,sum,orderId);
        positionBySymbol.setPositionKey(cusip);
        collector.collect(positionBySymbol);
    }
}
