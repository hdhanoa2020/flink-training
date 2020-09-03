package org.apache.flink.training.assignments.assigners;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.Order;


public class OrderTimeStampAssigner implements AssignerWithPeriodicWatermarks<Order> {
    private final long allowedLatetime = 0; //try different scenarios
    private long currentMaxTimestamp =0;

    @Override
    public long extractTimestamp(Order element, long previousElementTimestamp) {
        // return element.getTimeStamp() ==0 ? System.currentTimeMillis() : element.getTimeStamp();
        long timestamp = element.getTimestamp() ==0 ? System.currentTimeMillis() : element.getTimestamp();
        // currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        return timestamp;
    }
    @Override
    public Watermark getCurrentWatermark() {
        // return new Watermark(currentMaxTimestamp - allowedLatetime);
        return new Watermark(System.currentTimeMillis());
    }

 /*   private final long maxOutOfOrderness = 3500; // 3.5 seconds
    private long currentMaxTimestamp;



    @Override
    public long extractTimestamp(Position element, long previousElementTimestamp) {
        long timestamp = element.getTimestamp() == 0 ? System.currentTimeMillis() : element.getTimestamp();
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }*/
}