package com.alibaba.ververica.cep.demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.alibaba.ververica.cep.demo.condition.EndCondition;
import com.alibaba.ververica.cep.demo.condition.StartCondition;
import com.alibaba.ververica.cep.demo.dynamic.JDBCPeriodicPatternProcessorDiscovererFactory;
import com.alibaba.ververica.cep.demo.event.Event;
import com.alibaba.ververica.cep.demo.event.EventDeSerializationSchema;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.alibaba.ververica.cep.demo.Constants.INPUT_TOPIC;
import static com.alibaba.ververica.cep.demo.Constants.INPUT_TOPIC_GROUP;
import static com.alibaba.ververica.cep.demo.Constants.JDBC_DRIVE;
import static com.alibaba.ververica.cep.demo.Constants.JDBC_INTERVAL_MILLIS;
import static com.alibaba.ververica.cep.demo.Constants.JDBC_URL;
import static com.alibaba.ververica.cep.demo.Constants.KAFKA_BROKERS;
import static com.alibaba.ververica.cep.demo.Constants.TABLE_NAME;

public class CepDemo {

    public static void printTestPattern(Pattern<?, ?> pattern) throws JsonProcessingException {
        System.out.println(CepJsonUtils.convertPatternToJSONString(pattern));
    }

    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Build Kafka source with new Source API based on FLIP-27
        KafkaSource<Event> kafkaSource =
                KafkaSource.<Event>builder()
                        .setBootstrapServers(KAFKA_BROKERS)
                        .setTopics(INPUT_TOPIC)
                        .setStartingOffsets(OffsetsInitializer.latest())
                        .setGroupId(INPUT_TOPIC_GROUP)
                        .setDeserializer(new EventDeSerializationSchema())
                        .build();
        // DataStream Source
        DataStreamSource<Event> source =
                env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> event.getEventTime()),
                        "Kafka Source");

        env.setParallelism(1);

        KeyedStream<Event, Tuple2<Integer, Integer>> keyedStream =
                source.keyBy(new KeySelector<Event, Tuple2<Integer, Integer>>() {

                    @Override
                    public Tuple2<Integer, Integer> getKey(Event value) throws Exception {
                        return Tuple2.of(value.getId(), value.getProductionId());
                    }
                });

        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(new StartCondition("action == 0"))
                        .timesOrMore(3)
                        .followedBy("end")
                        .where(new EndCondition());
        printTestPattern(pattern);

        // Dynamic CEP patterns
        SingleOutputStreamOperator<String> output =
                CEP.dynamicPatterns(
                        keyedStream,
                        new JDBCPeriodicPatternProcessorDiscovererFactory<>(
                                JDBC_URL, JDBC_DRIVE, TABLE_NAME, null, JDBC_INTERVAL_MILLIS),
                        TimeBehaviour.ProcessingTime,
                        TypeInformation.of(new TypeHint<String>() {
                        }));
        // Print output stream in taskmanager's stdout
        output.print();
        // Compile and submit the job
        env.execute("CEPDemo");
    }
}
