package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.BuySell;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
Calculate the total quantity quantity for every single cusip,account and subaccount
 */
public class AllocationWindowFunction extends ProcessWindowFunction<Position, Position, Tuple3<String,String,String>, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(AllocationWindowFunction.class);

    @Override
    public void process(Tuple3<String, String, String> tuple3, Context context, Iterable<Position> iterable, Collector<Position> collector) throws Exception {
        int sum =0;
        String orderId="";
        for(Position a : iterable){
             if(a.getBuySell().equals(BuySell.BUY)){
                 sum = sum + a.getQuantity();
             }else if(a.getBuySell().equals(BuySell.SELL)){
                 sum = sum - a.getQuantity();
            }
            orderId=a.getOrderId(); //get the last one to calculate latency (matrix)
            //LOG.debug("AllocationWindowFunction orderId: {}",orderId);
        }
        Position aa = new Position(tuple3.f0,BuySell.BUY,tuple3.f1, tuple3.f2,sum,orderId);
        collector.collect(aa);
    }
}