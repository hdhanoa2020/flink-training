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
import org.apache.flink.training.assignments.assigners.PositionWatermark;
import org.apache.flink.training.assignments.domain.ComplianceResult;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionBySymbol;
import org.apache.flink.training.assignments.functions.AllocationWindowFunction;
import org.apache.flink.training.assignments.functions.OrderFlatMap;
import org.apache.flink.training.assignments.functions.PostionsByCusip;
import org.apache.flink.training.assignments.serializers.ComplianceResultSerialization;
import org.apache.flink.training.assignments.serializers.OrderKafkaDeserialization;
import org.apache.flink.training.assignments.serializers.PositionBySymbolSerialization;
import org.apache.flink.training.assignments.serializers.PositionSerializationSchema;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaOrderAssignment extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignment.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.harpreet1.wsn.riskfocus.com:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "out";
    public static final String KAFKA_GROUP = "";
    public static final String OUT_POSITIONS_BY_SYM_TOPIC = "positionsBySymbol";
    public static final String OUT_POSITIONS_BY_ACT_TOPIC = "positionsByAct";


    public static void main(String[] args) throws Exception {

        // set up streaming execution environment with checkpoints
        //var env = setEnvAndCheckpoints();
        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining(); //it will improve grafana metrix view
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //set watermarker interval
       env.getConfig().setAutoWatermarkInterval(10000L); //10 sec

       //env.setParallelism(ExerciseBase.parallelism);  //its better to setup during deployment

        // Create tbe Kafka Consumer here
        FlinkKafkaConsumer010<Order> flinkKafkaConsumer = createKafkaConsumer(IN_TOPIC,KAFKA_ADDRESS,KAFKA_GROUP);
        var orderStream = env.addSource(flinkKafkaConsumer).name("KafkaOrderReader").uid("KafkaOrderReader");

        // Log results
        //orderStream.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, " orderStream : {}"));

        //flat the stream to get all the elements at the same level and assign watermarks
        DataStream<Position> accountAllocation = orderStream
               // .assignTimestampsAndWatermarks(new OrderAscendingWatermarker())
                 .flatMap(new OrderFlatMap()).name("flatAllocations").uid("flatAllocations");//.setParallelism(12);
                 // .assignTimestampsAndWatermarks(new PositionWatermark());
        //Log results
        //accountAllocation.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, " flatAllocations : {}"));

        //group by cusip,account and sub account
        DataStream<Position> positionsByAccount = CalculatePostionQty(accountAllocation);
        // Log results
       // positionsByAccount.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "positionsByActOutput: {}"));


        //group by cusip/symbol with list of allocations
        DataStream<PositionBySymbol> positionsBySymbol =  accountAllocation
                .assignTimestampsAndWatermarks(new PositionWatermark())//new AccountAllocationWatermark())//(new OrderAscendingWatermarker())
                .keyBy(trade -> trade.getCusip())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new PostionsByCusip()).name("PostionsBySymbol").uid("PostionsBySymbol");

        //positionsBySymbol.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, " positionsBySymbolOutput : {}"));

        //pass the order ID and timestamp for latency matrix
       /* DataStream<ComplianceResult>  results = positionsByAccount.map(new BuildTestResults());
        // sent to out topic for latency matrix
        FlinkKafkaProducer010 flinkKafkaProducerCompResults = createTestResultProducer(OUT_TOPIC,KAFKA_ADDRESS);
        results.addSink(flinkKafkaProducerCompResults);*/


        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducer = createKafkaProducer(OUT_POSITIONS_BY_ACT_TOPIC,KAFKA_ADDRESS);
        positionsByAccount.addSink(flinkKafkaProducer);

        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducerSym = createKafkaPostionProducer(OUT_POSITIONS_BY_SYM_TOPIC,KAFKA_ADDRESS);
        positionsBySymbol.addSink(flinkKafkaProducerSym);

        // execute the transformation pipeline
        env.execute("kafkaOrders");
    }

    private static DataStream<Position> CalculatePostionQty(DataStream<Position> accountAllocation){
        return accountAllocation.
                assignTimestampsAndWatermarks(new PositionWatermark()).//new AccountAllocationWatermark()).
                keyBy(
                new KeySelector<Position, Tuple3<String, String,String>>() {
                    @Override
                    public Tuple3<String, String,String> getKey(Position value) throws Exception {
                        return Tuple3.of(value.getCusip(), value.getAccount(),value.getSubAccount());
                    }
                }
        ).window(TumblingEventTimeWindows.of(Time.seconds(10))) //Time.seconds(4)
                .process(new AllocationWindowFunction()).name("positionsByAccount").uid("positionsByAccount");

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