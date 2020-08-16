package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionBySymbol;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PostionsByCusip extends ProcessWindowFunction<Position, PositionBySymbol, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(PostionsByCusip.class);

    @Override
    public void process(String cusip, Context context, Iterable<Position> iterable, Collector<PositionBySymbol> collector) throws Exception {
        int sum =0;
        String orderId="";
        for(Position a : iterable){
            if(a.getBuySell().equals(BuySell.BUY)){
                sum = sum + a.getQuantity();
            }else if(a.getBuySell().equals(BuySell.SELL)){
                sum = sum - a.getQuantity();
            }
            orderId=a.getOrderId(); //get the last one to calculate latency (matrix)
        }
        PositionBySymbol positionBySymbol = new PositionBySymbol(cusip,sum,orderId);
        collector.collect(positionBySymbol);
    }
}
