package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.training.assignments.assigners.PositionWatermark;
import org.apache.flink.training.assignments.domain.*;
import org.apache.flink.training.assignments.functions.*;
import org.apache.flink.training.assignments.serializers.*;
import org.apache.flink.training.assignments.sinks.LogSink;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaMrkValueCalculator extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOrderAssignment.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.harpreet1.wsn.riskfocus.com:9092";
    public static final String IN_TOPIC = "in";
    public static final String OUT_TOPIC = "out";
    public static final String KAFKA_GROUP = "";
    public static final String OUT_POSITIONS_BY_SYM_TOPIC = "positionsBySymbol";
    public static final String OUT_POSITIONS_BY_ACT_TOPIC = "positionsByAct";
    public static final String PRICE_TOPIC = "price";
    public static final String MV_BY_ACT_TOPIC = "mvByAct";
    public static final String MV_BY_SYMBOL_TOPIC = "mvBySymbol";


    public static void main(String[] args) throws Exception {
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(10000L); //10 sec

        env.disableOperatorChaining();

        // Create tbe Kafka Consumer here
        FlinkKafkaConsumer010<Order> flinkKafkaConsumer = createKafkaConsumer(IN_TOPIC,KAFKA_ADDRESS,KAFKA_GROUP);
        var orderStream = env.addSource(flinkKafkaConsumer).name("KafkaOrderReader").uid("KafkaOrderReader");
       // orderStream.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, " orderStream : {}"));

        // Create tbe Kafka Consumer for price topic
        FlinkKafkaConsumer010<Price> flinkKafkaConsumerPrice = createKafkaConsumerPrice(PRICE_TOPIC,KAFKA_ADDRESS,KAFKA_GROUP);
        var  priceInputs = env.addSource(flinkKafkaConsumerPrice).name("KafkaPriceReader").uid("KafkaPriceReader");
        DataStream<Price> priceStream = priceInputs.keyBy(price -> price.getCusip());
       // priceStream.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, " priceStreamLog : {}"));


        //flat the stream to get all the elements at the same level and assign watermarks
        DataStream<Position> accountAllocation = orderStream
                // .assignTimestampsAndWatermarks(new OrderAscendingWatermarker())
                .flatMap(new OrderFlatMap()).name("splitAllocations").uid("splitAllocations");
               //  .assignTimestampsAndWatermarks(new PositionWatermark());
       // accountAllocation.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "splitAllocationsL : {}"));

        //group by cusip,account and sub account
        DataStream<Position> positionsByAccount = CalculatePostionQtyByAccount(accountAllocation);
        positionsByAccount.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "positionsByActOutput: {}"));

        //group by symbol with list of allocations
        DataStream<PositionBySymbol> positionsBySymbol =  CalculatePostionQtyBySymbol(accountAllocation);
        positionsBySymbol.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, " positionsBySymbolOutput : {}"));

        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducer = createKafkaProducer(OUT_POSITIONS_BY_ACT_TOPIC,KAFKA_ADDRESS);
        positionsByAccount.addSink(flinkKafkaProducer);
        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducerSym = createKafkaPostionProducer(OUT_POSITIONS_BY_SYM_TOPIC,KAFKA_ADDRESS);
        positionsBySymbol.addSink(flinkKafkaProducerSym);

//calculate Market Value

        DataStream<Position> accountPositions =positionsByAccount.keyBy(position -> position.getCusip());
        DataStream<Position> mkvByAccount = priceStream
                .connect(accountPositions)
                .flatMap(new AccountLeveMrkValueFlatMap())
                .uid("AccountLeveMrkValueOutput").name("AccountLeveMrkValueOutput");
        mkvByAccount.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "MarketValueByAct: {}"));

        //pass the order ID and timestamp for latency matrix
        DataStream<ComplianceResult>  results = mkvByAccount.map(new BuildTestResults());
        results.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "TestResults: {}"));

        //join the positionBySymbol and Price
        DataStream<PositionBySymbol> symbolPositions = positionsBySymbol.keyBy(position -> position.getCusip());
        DataStream<PositionBySymbol> mkvBySymbol = priceStream
                .connect(symbolPositions)
                .flatMap(new CusipLeveMrkValueFlatMap())
                .uid("CusipLeveMrkValueOutput").name("CusipLeveMrkValueOutput");;
         mkvBySymbol.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "MarketValueBySymbol: {}"));


        // sent to out topic for latency matrix
        FlinkKafkaProducer010 flinkKafkaProducerCompResults = createTestResultProducer(OUT_TOPIC,KAFKA_ADDRESS);
        results.addSink(flinkKafkaProducerCompResults);

        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducerMrkValue = createMrkValueAccountProducer(MV_BY_ACT_TOPIC,KAFKA_ADDRESS);
        mkvByAccount.addSink(flinkKafkaProducerMrkValue);

        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducerBySym = createMrkValueSymbolProducer(MV_BY_SYMBOL_TOPIC,KAFKA_ADDRESS);
        mkvBySymbol.addSink(flinkKafkaProducerBySym);


        // execute the transformation pipeline
        env.execute("KafkaMVCalculator");
    }

    private static DataStream<Position> CalculatePostionQtyByAccount(DataStream<Position> accountAllocation){
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
    private static DataStream<PositionBySymbol> CalculatePostionQtyBySymbol(DataStream<Position> accountAllocation){
        return accountAllocation
                .assignTimestampsAndWatermarks(new PositionWatermark())//new AccountAllocationWatermark())//(new OrderAscendingWatermarker())
                .keyBy(trade -> trade.getCusip())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new PostionsByCusip()).name("PostionsBySymbol").uid("PostionsBySymbol");
    }

    public static FlinkKafkaConsumer010<Order> createKafkaConsumer(String topic, String kafkaAddress,String group)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        prop.setProperty("group.id",group);
        var stringConsumer = new FlinkKafkaConsumer010<Order>(topic,new OrderKafkaDeserialization(),prop);
        return stringConsumer;

    }
    public static FlinkKafkaConsumer010<Price> createKafkaConsumerPrice(String topic, String kafkaAddress,String group)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        prop.setProperty("group.id",group);

        var stringConsumer = new FlinkKafkaConsumer010<Price>(topic,new PriceKafkaDeserialization(),prop);
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
    public static FlinkKafkaProducer010<Position> createMrkValueAccountProducer(String topic, String kafkaAddress)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        var producer010 = new FlinkKafkaProducer010<Position>(topic,new AccountAllocationSerializationSchema(), prop);
        return producer010;
    }
    public static FlinkKafkaProducer010<PositionBySymbol> createMrkValueSymbolProducer(String topic, String kafkaAddress)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        var producer010 = new FlinkKafkaProducer010<PositionBySymbol>(topic,new PositionBySymbolSerialization(), prop);
        return producer010;
    }


}