package com.alibaba.ververica.cep.demo.condition;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;

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
        if (event.getAction() == 1 && event.getListeningTime() >= 40) {
            return true;
        }
        long sum = 0;
        for (Event e : context.getEventsForPattern(nameOfListeningMusicPattern)) {
            sum += e.getListeningTime();
        }
        if (event.getAction() == 1 && sum + event.getListeningTime() >= 40) return true;
        return sum >= thresholdOfReward;
    }
}
