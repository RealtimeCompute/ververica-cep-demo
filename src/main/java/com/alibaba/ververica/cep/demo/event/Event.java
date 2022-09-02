package com.alibaba.ververica.cep.demo.event;

import java.util.Objects;

/** Exemplary event for usage in tests of CEP. See also {@link SubEvent} */
public class Event {
    private final int id;
    private final String name;
    private final double amount;
    private final long eventTime;

    public Event(int id, String name, double price, long timestamp) {
        this.id = id;
        this.name = name;
        this.amount = price;
        this.eventTime = timestamp;
    }

    public static Event fromString(String eventStr) {
        String[] split = eventStr.split(",");
        return new Event(
                Integer.parseInt(split[0]),
                split[1],
                Double.parseDouble(split[2]),
                Long.parseLong(split[3]));
    }

    public long getEventTime() {
        return eventTime;
    }

    public double getAmount() {
        return amount;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Event(" + id + ", " + name + ", " + amount + ", " + eventTime + ")";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Event) {
            Event other = (Event) obj;
            return name.equals(other.name) && amount == other.amount && id == other.id;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, amount, id);
    }
}
