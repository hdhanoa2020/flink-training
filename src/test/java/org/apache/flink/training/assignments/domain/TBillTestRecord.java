package org.apache.flink.training.assignments.domain;

import org.apache.flink.api.java.tuple.Tuple2;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class TBillTestRecord {
    public List<Tuple2<LocalDateTime, Double>> returns = new ArrayList<>();
    public Double average;
    public Double volatility;
}
