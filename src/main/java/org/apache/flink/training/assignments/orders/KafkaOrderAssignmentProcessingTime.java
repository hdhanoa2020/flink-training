package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.domain.ComplianceResult;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionBySymbol;
import org.apache.flink.training.assignments.functions.OrderCusipMap;
import org.apache.flink.training.assignments.functions.OrderFlatMap;
import org.apache.flink.training.assignments.serializers.ComplianceResultSerialization;
import org.apache.flink.training.assignments.serializers.OrderKafkaDeserialization;
import org.apache.flink.training.assignments.serializers.PositionBySymbolSerialization;
import org.apache.flink.training.assignments.serializers.PositionSerializationSchema;
import org.apache.flink.training.assignments.sinks.LogSink;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaOrderAssignmentProcessingTime extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignmentProcessingTime.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.harpreet1.wsn.riskfocus.com:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "out";
    public static final String KAFKA_GROUP = "";
    public static final String OUT_POSITIONS_BY_SYM_TOPIC = "positionsBySymbol";
    public static final String OUT_POSITIONS_BY_ACT_TOPIC = "positionsByAct";


    public static void main(String[] args) throws Exception {

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.disableOperatorChaining();

        // Create tbe Kafka Consumer here
        FlinkKafkaConsumer010<Order> flinkKafkaConsumer = createKafkaConsumer(IN_TOPIC,KAFKA_ADDRESS,KAFKA_GROUP);
        var orderStream = env.addSource(flinkKafkaConsumer).name("KafkaOrderReader").uid("KafkaOrderReader")
                .keyBy(order -> order.getCusip());


        //flat the stream to get all the elements at the same level and assign watermarks
        DataStream<Position> accountAllocation = orderStream
                 .flatMap(new OrderFlatMap()).name("splitAllocations").uid("splitAllocations");

        //group by cusip,account and sub account
        DataStream<Position> positionsByAccount = CalculatePostionQty(accountAllocation);

        // Log results
        positionsByAccount.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "positionsByActOutput: {}"));

        /**
         * convert orders to  Positions by Cusip and publish to kafka
         */
        DataStream<Position> positionsBySymbol = aggregatePositionsBySymbol(orderStream)
                .keyBy(positionByCusip -> positionByCusip.getCusip())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sum("quantity")
                .name("AddPositionBySymbol")
                .uid("AddPositionBySymbol");

        positionsBySymbol.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, " positionsBySymbolOutput : {}"));



        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducer = createKafkaProducer(OUT_POSITIONS_BY_ACT_TOPIC,KAFKA_ADDRESS);
        positionsByAccount.addSink(flinkKafkaProducer)
                .name("FinalPositionsByAcctToKafka")
                .uid("FinalPositionsByAcctToKafka");

        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducerSym = createKafkaProducer(OUT_POSITIONS_BY_SYM_TOPIC,KAFKA_ADDRESS);
        positionsBySymbol.addSink(flinkKafkaProducerSym)
                .name("FinalPositionsBySymbolToKafka")
                .uid("FinalPositionsBySymbolToKafka");

        System.out.println(env.getExecutionPlan());
        // execute the transformation pipeline
        env.execute("kafkaOrders");
    }

    private static DataStream<Position> CalculatePostionQty(DataStream<Position> accountAllocation){
        return accountAllocation.
                keyBy(
                new KeySelector<Position, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> getKey(Position value) throws Exception {
                        return Tuple3.of(value.getCusip(), value.getAccount(),value.getSubAccount());
                    }
                }
        )
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sum("quantity")
                .name("TotalPositionsQtyByAccount")
                .uid("TotalPositionsQtyByAccount");

    }


    public static FlinkKafkaConsumer010<Order> createKafkaConsumer(String topic, String kafkaAddress,String group)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        prop.setProperty("group.id",group);
        var stringConsumer = new FlinkKafkaConsumer010<Order>(topic,new OrderKafkaDeserialization(),prop);
        return stringConsumer;

    }
    public static FlinkKafkaProducer010<ComplianceResult> createTestResultProducer(String topic, String kafkaAddress)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        var producer010 = new FlinkKafkaProducer010<ComplianceResult>(topic,new ComplianceResultSerialization(), prop);
        return producer010;
    }

    public static FlinkKafkaProducer010<Position> createKafkaProducer(String topic, String kafkaAddress)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);

        var producer010 = new FlinkKafkaProducer010<Position>(topic,new PositionSerializationSchema(), prop);
        return producer010;

    }
    public static FlinkKafkaProducer010<PositionBySymbol> createKafkaPostionProducer(String topic, String kafkaAddress)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        var producer010 = new FlinkKafkaProducer010<PositionBySymbol>(topic,new PositionBySymbolSerialization(), prop);
        return producer010;
    }

    private static DataStream<Position> aggregatePositionsBySymbol(DataStream<Order> orderStream) {
        var cusipPositions =  orderStream
                .map(new OrderCusipMap())
                .name("ConvertOrdersToPositionByCusip")
                .uid("ConvertOrdersToPositionByCusip");
        return cusipPositions;
    }

    public static StreamExecutionEnvironment setEnvAndCheckpoints(){
        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
        conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.enableCheckpointing(10000L);
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        return env;

    }

}