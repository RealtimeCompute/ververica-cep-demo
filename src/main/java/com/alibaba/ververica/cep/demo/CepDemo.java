package com.alibaba.ververica.cep.demo;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import com.alibaba.ververica.cep.demo.condition.EndCondition;
import com.alibaba.ververica.cep.demo.condition.MiddleCondition;
import com.alibaba.ververica.cep.demo.condition.StartCondition;
import com.alibaba.ververica.cep.demo.event.Event;

import java.util.List;
import java.util.Map;

public class CepDemo {

    public static void printTestPattern(Pattern<?, ?> pattern) throws JsonProcessingException {
        System.out.println(CepJsonUtils.convertPatternToJSONString(pattern));
    }

    public static void checkArg(String argName, MultipleParameterTool params) {
        if (!params.has(argName)) {
            throw new IllegalArgumentException(argName + " must be set!");
        }
    }

    public static void main(String[] args) throws Exception {
        // Process args
        //        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        //        checkArg(KAFKA_BROKERS_ARG, params);
        //        checkArg(INPUT_TOPIC_ARG, params);
        //        checkArg(INPUT_TOPIC_GROUP_ARG, params);
        //        checkArg(JDBC_URL_ARG, params);
        //        checkArg(TABLE_NAME_ARG, params);
        //        checkArg(JDBC_INTERVAL_MILLIS_ARG, params);

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Build Kafka source with new Source API based on FLIP-27
        //        KafkaSource<Event> kafkaSource =
        //                KafkaSource.<Event>builder()
        //                        .setBootstrapServers(params.get(KAFKA_BROKERS_ARG))
        //                        .setTopics(params.get(INPUT_TOPIC_ARG))
        //                        .setStartingOffsets(OffsetsInitializer.latest())
        //                        .setGroupId(params.get(INPUT_TOPIC_GROUP_ARG))
        //                        .setDeserializer(new EventDeSerializationSchema())
        //                        .build();
        // DataStream Source
        //        DataStreamSource<Event> source =
        //                env.fromSource(
        //                        kafkaSource,
        //                        WatermarkStrategy.<Event>forMonotonousTimestamps()
        //                                .withTimestampAssigner((event, ts) ->
        // event.getEventTime()),
        //                        "Kafka Source");
        // DataStream Source
        // action 0 : accept task
        // action 1 : listen music
        DataStreamSource<Event> source =
                env.fromElements(
                        new Event(1, "barfoo", 0, 0, 1),
                        new Event(1, "barfoo", 1, 40, 1),
                        new Event(1, "barfoo", 0, 0, 1),
                        new Event(1, "barfoo", 1, 40, 1),
                        //                        new Event(1, "barfoo", 1, 40, 1),
                        new Event(1, "dd", 0, 0, 1),
                        new Event(1, "dd", 1, 20, 1),
                        new Event(1, "dd", 1, 11, 1),
                        new Event(1, "dd", 1, 19, 1),
                        //                        new Event(1, "e", 0, 0, 1),
                        new Event(1, "e", 0, 0, 1),
                        //                        new Event(1, "e", 0, 0, 1),
                        new Event(1, "e", 1, 20, 1),
                        new Event(1, "e", 1, 11, 1),
                        new Event(1, "e", 1, 22, 1),
                        new Event(1, "e2", 0, 0, 1),
                        //                        new Event(1, "e2", 0, 0, 1),
                        new Event(1, "e2", 1, 20, 1),
                        new Event(1, "e2", 1, 11, 1),
                        new Event(1, "e2", 1, 3, 1),
                        new Event(1, "e2", 2, 0, 1),
                        new Event(1, "e2", 1, 40, 1));

        env.setParallelism(1);

        KeyedStream<Event, Tuple2<Integer, Integer>> keyedStream =
                source.keyBy(
                        new KeySelector<Event, Tuple2<Integer, Integer>>() {

                            @Override
                            public Tuple2<Integer, Integer> getKey(Event value) throws Exception {
                                return Tuple2.of(value.getId(), value.getId());
                            }
                        });

        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(new StartCondition())
                        .followedBy("listeningMusic")
                        .where(new MiddleCondition())
                        .oneOrMore()
                        .optional()
                        .followedBy("end")
                        .where(new EndCondition("listeningMusic", 40))
                        .within(Time.days(1));
        //        printTestPattern(pattern);

        // Dynamic CEP patterns
        PatternStream<Event> patternStream = CEP.pattern(keyedStream, pattern).inProcessingTime();
        SingleOutputStreamOperator<Row> output =
                patternStream.select(
                        new PatternSelectFunction<Event, Row>() {
                            @Override
                            public Row select(Map<String, List<Event>> map) throws Exception {
                                List<Event> start = map.get("start");
                                Event startEvent = start.get(0);
                                System.out.println("reward for " + startEvent.getName());
                                Event endEvent = start.get(start.size() - 1);
                                return Row.of(
                                        startEvent.getId(),
                                        startEvent.getEventTime(),
                                        endEvent.getEventTime());
                            }
                        });
        // Print output stream in taskmanager's stdout
        output.print();
        // Compile and submit the job
        env.execute("CEPDemo");
    }
}
