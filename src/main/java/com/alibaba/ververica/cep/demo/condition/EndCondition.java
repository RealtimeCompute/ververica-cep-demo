package com.alibaba.ververica.cep.demo.condition;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import com.alibaba.ververica.cep.demo.event.Event;

public class EndCondition extends IterativeCondition<Event> {

    private final String nameOfListeningMusicPattern;
    private final long thresholdOfReward;

    public EndCondition(String nameOfListeningMusicPattern, long thresholdOfReward) {
        this.nameOfListeningMusicPattern = nameOfListeningMusicPattern;
        this.thresholdOfReward = thresholdOfReward;
    }


    @Override
    public boolean filter(Event event, Context<Event> context) throws Exception {
        long sum = 0;
//        System.out.println(nameOfListeningMusicPattern);
        for (Event e : context.getEventsForPattern(nameOfListeningMusicPattern)) {
//            System.out.println(sum);
            sum += e.getListeningTime();
        }
        return sum >= thresholdOfReward;
    }
}
