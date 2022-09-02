package com.alibaba.ververica.cep.demo.condition;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import com.alibaba.ververica.cep.demo.event.SubEvent;

public class SubEndCondition extends SimpleCondition<SubEvent> {

    @Override
    public boolean filter(SubEvent value) throws Exception {
        return value.getName().equals("subEnd");
    }
}
