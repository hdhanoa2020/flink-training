package org.apache.flink.training.assignments.assigners;


import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.training.assignments.domain.Position;

public class AccountAllocationWatermark extends AscendingTimestampExtractor<Position> {

    @Override
    public long extractAscendingTimestamp(Position position) {
        return System.currentTimeMillis()-2;
    }
}