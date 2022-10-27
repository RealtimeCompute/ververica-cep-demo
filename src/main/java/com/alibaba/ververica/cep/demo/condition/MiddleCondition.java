package com.alibaba.ververica.cep.demo.condition;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import com.alibaba.ververica.cep.demo.event.Event;

public class MiddleCondition extends IterativeCondition<Event> {


    @Override
    public boolean filter(Event event, Context<Event> context) throws Exception {
//        System.out.println("m");
//        System.out.println(event.getAction() == 1);
        return event.getAction() == 1;
    }
}
