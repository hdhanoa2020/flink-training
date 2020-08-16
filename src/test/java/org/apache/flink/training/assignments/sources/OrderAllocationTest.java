/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.assignments.sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.OrderTestResults;
//import org.apache.flink.training.assignments.domain.TBillRate;
//import org.apache.flink.training.assignments.domain.TBillTestResults;
import org.apache.flink.training.assignments.orders.KafkaOrderAssignment;
import org.apache.flink.training.assignments.orders.TestBase;
import org.apache.flink.training.assignments.orders.Testable;
//import org.apache.flink.training.assignments.sinks.AverageSink;
//import org.apache.flink.training.assignments.sinks.PriceSink;
//import org.apache.flink.training.assignments.sinks.TestSink;
//import org.apache.flink.training.assignments.sinks.VolatilitySink;
//import org.apache.flink.training.assignments.sources.TestRateSource;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OrderAllocationTest extends TestBase<Order,Tuple3<LocalDateTime, Double,Boolean>> {
    private static final Logger LOG = LoggerFactory.getLogger(OrderAllocationTest.class);

    private static final String[] testMonths = new String[]{"2020-03"};
    private static final Testable TEST_APPLICATION = () -> KafkaOrderAssignment.main(testMonths);
    private static final DecimalFormat df = new DecimalFormat("0.00000E00");
    private static OrderTestResults testResults = null;

    @BeforeAll
    public static void readAnswers() {
        testResults = OrderTestResults.ofResource("/tbill_test_data.csv");
    }

  /*  @Test
    public void testReturns() throws Exception {
        TestRateSource source = new TestRateSource();

        List<Tuple2<LocalDateTime, Double>> expectedList = testResults.resultMap.get(testMonths[0]).returns;
        List<Tuple3<LocalDateTime, Double,Boolean>> actualList  = results(source,new PriceSink());
        List<Tuple2<LocalDateTime, Double>> convertedActualList  = new ArrayList<Tuple2<LocalDateTime, Double>>();

        for (Tuple3<LocalDateTime, Double,Boolean> t: actualList ){
            //LOG.info("orignal price value : {} converted price value : {}",t.f1,Double.valueOf(df.format(t.f1)));
            convertedActualList.add(Tuple2.of(t.f0,Double.valueOf(df.format(t.f1))));
        }
        LOG.info("expected price : {}",testResults.resultMap.get(testMonths[0]).returns,convertedActualList);
        LOG.info("actual price   : {} ",convertedActualList);
        assertEquals(testResults.resultMap.get(testMonths[0]).returns, convertedActualList);
    }

    @Test
    public void testAverages() throws Exception {

        double expectedAvg = testResults.resultMap.get(testMonths[0]).average;
        TestRateSource source = new TestRateSource();
        List<Tuple3<LocalDateTime, Double,Boolean>> actualAvg  = results(source,new AverageSink());
        LOG.info("expectedAvg  : {}  actualAvg  : {}",expectedAvg,Double.valueOf(df.format( actualAvg.get(0).f1)));
        assertEquals(expectedAvg,Double.valueOf(df.format( actualAvg.get(0).f1)));
    }

    @Test
    public void testVolatility() throws Exception {
        double expectedVolatility = testResults.resultMap.get(testMonths[0]).volatility;
        TestRateSource source = new TestRateSource();
        List<Tuple3<LocalDateTime, Double,Boolean>> actualVolatility  = results(source,new VolatilitySink());
        LOG.info("expectedVolatility :  {}: ",expectedVolatility);
        LOG.info("actualVolatility : {}: ",actualVolatility);
        assertEquals(expectedVolatility, Double.valueOf(df.format( actualVolatility.get(0).f1)));

    }

    protected List<Tuple3<LocalDateTime, Double,Boolean>> results(TestRateSource source) throws Exception {
        return runApp(source, new TestSink<List<Tuple3<LocalDateTime, Double,Boolean>>>(), TEST_APPLICATION);
    }

    protected List<Tuple3<LocalDateTime, Double,Boolean>> results(TestRateSource source, PriceSink sink) throws Exception {
        return runApp(source, sink, TEST_APPLICATION);
    }
    protected List<Tuple3<LocalDateTime, Double,Boolean>> results(TestRateSource source, AverageSink sink) throws Exception {
        return runApp(source, sink, TEST_APPLICATION);
    }
    protected List<Tuple3<LocalDateTime, Double,Boolean>> results(TestRateSource source, VolatilitySink sink) throws Exception {
        return runApp(source, sink, TEST_APPLICATION);
    }
*/

}
