package com.alibaba.ververica.cep.demo.condition;

import com.alibaba.ververica.cep.demo.event.Event;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

public class StartCondition extends SimpleCondition<Event> {


    @Override
    public boolean filter(Event event) throws Exception {
//        System.out.println(event.getAction() == 0);
        return event.getAction() == 0;
    }
}
