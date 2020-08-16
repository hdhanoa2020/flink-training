package org.apache.flink.training.assignments.assigners;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.Position;


public class PositionWatermark implements AssignerWithPeriodicWatermarks<Position> {
    private final long allowedLatetime = 0; //try different scenarios
    private long currentMaxTimestamp =0;

    @Override
    public long extractTimestamp(Position element, long previousElementTimestamp) {
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
}