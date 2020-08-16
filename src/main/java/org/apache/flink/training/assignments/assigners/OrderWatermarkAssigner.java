package org.apache.flink.training.assignments.assigners;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.Order;

import javax.annotation.Nullable;

public class OrderWatermarkAssigner implements AssignerWithPeriodicWatermarks<Order> {
    private long  currentMaxtimestamp;;
    private final long  allowedLatetime = 0; // latency

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxtimestamp-allowedLatetime);
    }

    @Override
    public long extractTimestamp(Order element, long previousElementTimestamp) {
        long timestamp = element.getTimestamp();
        currentMaxtimestamp = Math.max(currentMaxtimestamp, element.getTimestamp());
        return timestamp;
    }

}
