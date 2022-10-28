package com.alibaba.ververica.cep.demo.condition;

import org.apache.flink.cep.pattern.conditions.IterativeCondition;

import com.alibaba.ververica.cep.demo.event.Event;

public class MiddleCondition extends IterativeCondition<Event> {

    @Override
    public boolean filter(Event event, Context<Event> context) throws Exception {
        //        System.out.println("m");
        //        System.out.println(event.getAction() == 1);
        //        if (event.getAction() == 1) {
        //            long sum = 0;
        ////        System.out.println(nameOfListeningMusicPattern);
        //            for (Event e : context.getEventsForPattern("listeningMusic")) {
        ////            System.out.println(sum);
        //                sum += e.getListeningTime();
        //            }
        //            if (sum + event.getListeningTime() > 40) return true;
        //
        //        }
        return event.getAction() == 1 && event.getListeningTime() < 40;
    }
}
