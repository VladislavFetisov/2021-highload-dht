package ru.mail.polis.service.vladislav_fetisov.replication;

import ru.mail.polis.service.vladislav_fetisov.topology.Topology;

public class ReplicasManager {
    private final int ack;
    private final int from;
    private final int indexOfPort;
    private final int indexOfOurPort;
    private final Topology topology;

    private ReplicasManager(int ack, int from, Topology topology, int indexOfPort, int indexOfOurPort) {
        this.ack = ack;
        this.from = from;
        this.topology = topology;
        this.indexOfPort = indexOfPort;
        this.indexOfOurPort = indexOfOurPort;
    }

    public static ReplicasManager parseReplicas(
            String replicas, Topology topology, int indexOfPort, int indexOfOurPort) throws IllegalArgumentException {
        if (replicas == null) {
            return new ReplicasManager(
                    topology.getQuorum(), topology.getSortedPorts().length, topology, indexOfPort, indexOfOurPort);
        }
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
        return new ReplicasManager(ack, from, topology, indexOfPort, indexOfOurPort);
    }

    public boolean currentNodeIsReplica() {
        int rightBoundIndex = (indexOfPort + from - 1) % topology.getSortedPorts().length;
        if (rightBoundIndex < indexOfPort) {
            return indexOfOurPort >= indexOfPort || indexOfOurPort <= rightBoundIndex;
        } else {
            return indexOfOurPort >= indexOfPort && indexOfOurPort <= rightBoundIndex;
        }
    }

    public int getAck() {
        return ack;
    }

    public int getFrom() {
        return from;
    }

    public int getIndexOfPort() {
        return indexOfPort;
    }

    public int getIndexOfOurPort() {
        return indexOfOurPort;
    }
}
