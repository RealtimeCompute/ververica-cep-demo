package com.alibaba.ververica.cep.demo;

import com.alibaba.ververica.cep.demo.condition.EndCondition;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.cep.demo.condition.DebugCondition;
import com.alibaba.ververica.cep.demo.event.Event;

import java.util.List;
import java.util.Map;

public class DebugDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Output: 123
//        Pattern<Event, ?> pattern =
//                Pattern.<Event>begin("debug", AfterMatchSkipStrategy.skipPastLastEvent())
//                        .where(new DebugCondition())
//                        .timesOrMore(3)
//                        .within(Time.milliseconds(1000));
        // Output: 12345
                Pattern<Event, ?> pattern =
                        Pattern.<Event>begin("debug", AfterMatchSkipStrategy.skipPastLastEvent())
                                .where(new DebugCondition())
                                .timesOrMore(3)
                                .followedBy("end")
                                .where(new EndCondition())
                                .within(Time.milliseconds(1000));

        WatermarkStrategy<Event> watermarkStrategy =
                WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                (SerializableTimestampAssigner<Event>)
                                        (event, l) -> event.getEventTime());

        DataStream<Event> input =
                env.fromElements(
                                new Event(1, "debug", 1, 1, 1),
                                new Event(2, "debug", 1, 1, 1),
                                new Event(3, "debug", 1, 1, 2),
                                new Event(4, "debug", 1, 1, 3),
                                new Event(-1, "noise", 0, 1, 5),
                                new Event(5, "debug", 1, 1, 14),
                                new Event(18, "fine", 0, 1, 200))
                        .assignTimestampsAndWatermarks(watermarkStrategy);
        DataStream<String> result =
                CEP.pattern(input, pattern)
                        .inEventTime()
                        .process(
                                new PatternProcessFunction<Event, String>() {
                                    @Override
                                    public void processMatch(
                                            Map<String, List<Event>> match,
                                            Context context,
                                            Collector<String> collector) {
                                        StringBuilder sb = new StringBuilder();
                                        for (Event e : match.get("debug")) {
                                            sb.append(e.getId());
                                        }
                                        collector.collect(sb.toString());
                                    }
                                });
        result.print();
        env.execute("debug");
    }
}
