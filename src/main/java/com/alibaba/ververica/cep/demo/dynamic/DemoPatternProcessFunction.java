package com.alibaba.ververica.cep.demo.dynamic;

import org.apache.flink.cep.dynamic.operator.DynamicCepOperator;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class DemoPatternProcessFunction<IN> extends PatternProcessFunction<IN, String> {
    String id;
    int version;

    public DemoPatternProcessFunction(String id, int version) {
        this.id = id;
        this.version = version;
    }

    @Override
    public void processMatch(
            final Map<String, List<IN>> match, final Context ctx, final Collector<String> out) {
        StringBuilder sb = new StringBuilder();
        sb.append("A match for Pattern of (id, version): (")
                .append(id)
                .append(", ")
                .append(version)
                .append(") is found. The event sequence: ");
        for (Map.Entry<String, List<IN>> entry : match.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue());
        }
        out.collect(sb.toString());
    }
}
