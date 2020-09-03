package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.assigners.OrderTimeStampAssigner;
import org.apache.flink.training.assignments.domain.ComplianceResult;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionBySymbol;
import org.apache.flink.training.assignments.functions.AllocationWindowFunction;
import org.apache.flink.training.assignments.functions.OrderCusipSumWindow;
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

public class KafkaBlockOrder extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignmentProcessingTime.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.harpreet1.wsn.riskfocus.com:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "out";
    public static final String KAFKA_GROUP = "";
    public static final String OUT_POSITIONS_BY_SYM_TOPIC = "positionsBySymbol";
    public static final String OUT_POSITIONS_BY_ACT_TOPIC = "positionsByAct";


    public static void main(String[] args) throws Exception {

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.getConfig().setAutoWatermarkInterval(100000L); //10 sec
        //env.disableOperatorChaining();

        // Create tbe Kafka Consumer here
        FlinkKafkaConsumer010<Order> flinkKafkaConsumer = createKafkaConsumer(IN_TOPIC,KAFKA_ADDRESS,KAFKA_GROUP);
        var orderStream = env.addSource(flinkKafkaConsumer).keyBy(trade -> trade.getCusip()).
                assignTimestampsAndWatermarks(new OrderTimeStampAssigner()).
                name("OrderStream").uid("OrderStream");;// name("KafkaOrderReader").uid("KafkaOrderReader");//.keyBy(trade -> trade.getCusip());

        //flat the stream to get all the elements at the same level and assign watermarks
        DataStream<Position> accountAllocation = orderStream
                .keyBy(trade -> trade.getCusip())
                .flatMap(new OrderFlatMap()).name("splitAllocations").uid("splitAllocations");

        //group by cusip,account and sub account
        DataStream<Position> positionsByAccount = CalculatePostionQty(accountAllocation);
        positionsByAccount.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "positionsByActOutput: {}"));

        DataStream<Position> positionsBySymbol =  CalculatePostionQtyBySymbol(orderStream);
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

        /*/send output to out topic for comprison
        FlinkKafkaProducer010 flinkKafkaProducerTestResults = createKafkaPostionProducer(OUT_TOPIC,KAFKA_ADDRESS);
        FlinkKafkaProducer010 flinkKafkaProducerTestResults2 = createKafkaProducer(OUT_TOPIC,KAFKA_ADDRESS);
        positionsBySymbol.addSink(flinkKafkaProducerTestResults);
        positionsByAccount.addSink(flinkKafkaProducerTestResults2); */

        System.out.println(env.getExecutionPlan());
        env.execute("kafkaBlockOrders");
    }

    private static DataStream<Position> CalculatePostionQty(DataStream<Position> accountAllocation){
        return accountAllocation.
                // assignTimestampsAndWatermarks(new PositionWatermark()).
                        keyBy(
                        new KeySelector<Position, Tuple3<String, String,String>>() {
                            @Override
                            public Tuple3<String, String,String> getKey(Position value) throws Exception {
                                return Tuple3.of(value.getCusip(), value.getAccount(),value.getSubAccount());
                            }
                        }
                ).window(TumblingEventTimeWindows.of(Time.seconds(1))) // 1, 10
                .process(new AllocationWindowFunction())
                .name("TotalPositionsQtyByAccount")
                .uid("TotalPositionsQtyByAccount");

    }
    private static DataStream<Position> CalculatePostionQtyBySymbol(DataStream<Order> accountAllocation){
        return  accountAllocation
                .keyBy(trade -> trade.getCusip())
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .process(new OrderCusipSumWindow())
                .name("TotalPositionsQtyBySymbol").uid("TotalPositionsQtyBySymbol");
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