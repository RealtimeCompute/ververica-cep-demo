package com.alibaba.ververica.cep.demo.condition;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import com.alibaba.ververica.cep.demo.event.Event;

public class StartCondition extends SimpleCondition<Event> {

    @Override
    public boolean filter(Event event) throws Exception {
        //        System.out.println(event.getAction() == 0);
        if (event.getListeningTime() >= 40) {}

        return event.getAction() == 0;
    }
}
