package ru.mail.polis.service.vladislav_fetisov.replication;

import ru.mail.polis.service.vladislav_fetisov.topology.Topology;

public class Replicas {
    private final int ack;
    private final int from;

    private Replicas(int ack, int from) {
        this.ack = ack;
        this.from = from;
    }

    public static Replicas parseReplicas(String replicas, Topology topology) {
        int i = replicas.indexOf('/');
        int ack = Integer.parseInt(replicas.substring(0, i));
        int from = Integer.parseInt(replicas.substring(i + 1));
        if (from > topology.getSortedPorts().length || from <= 0) {
            throw new IllegalArgumentException("from is out of bounds, from = " + from);
        }
        if (ack <= 0 || ack > from) {
            throw new IllegalArgumentException(
                    String.format("ack is out of bounds, ack =%d where from = %d", ack, from));
        }
        return new Replicas(ack, from);
    }

    public int getFrom() {
        return from;
    }

    public int getAck() {
        return ack;
    }
}