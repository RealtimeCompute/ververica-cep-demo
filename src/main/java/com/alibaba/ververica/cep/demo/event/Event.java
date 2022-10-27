package com.alibaba.ververica.cep.demo.event;

import java.util.Objects;

/** Exemplary event for usage in tests of CEP. */
public class Event {
    private final int id;
    private final String name;

    private final int listeningTime;
    private final int action;
    private final long eventTime;

    public Event(int id, String name, int action, int listeningTime, long timestamp) {
        this.id = id;
        this.name = name;
        this.action = action;
        this.listeningTime = listeningTime;
        this.eventTime = timestamp;
    }

    public static Event fromString(String eventStr) {
        String[] split = eventStr.split(",");
        return new Event(
                Integer.parseInt(split[0]),
                split[1],
                Integer.parseInt(split[2]),
                Integer.parseInt(split[3]),
                Long.parseLong(split[4]));
    }

    public long getEventTime() {
        return eventTime;
    }

    public double getAction() {
        return action;
    }

    public int getId() {
        return id;
    }

    public int getListeningTime() {
        return listeningTime;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Event("
                + id
                + ", "
                + name
                + ", "
                + action
                + ", "
                + listeningTime
                + ", "
                + eventTime
                + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Event) {
            Event other = (Event) obj;
            return name.equals(other.name)
                    && action == other.action
                    && listeningTime == other.listeningTime
                    && id == other.id;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, action, listeningTime, id);
    }
}
