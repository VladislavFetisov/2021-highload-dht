package ru.mail.polis.service.vladislav_fetisov.topology;

import java.util.*;

public class VNode {
    //inclusive
    public final long lowBound;
    //inclusive
    public final long upperBound;

    public VNode(long lowBound, long upperBound) {
        this.lowBound = lowBound;
        this.upperBound = upperBound;
    }

    /**
     * node * vNodes > 1
     */
    public static VNode[] getAllVNodes(int nodes, int vNodes, Range range) {
        long num = (long) nodes * vNodes; //cast to long only for escape overflow
        if (num > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(String.format("Whole amount of VNodes: %d more than maxValue: %d",
                    num, Integer.MAX_VALUE));
        }
        long rangeUnit = range.maxValue / num;
        long remaining = range.maxValue % num + 1;
        if (remaining == num) {
            rangeUnit++;
            remaining = 0;
        }
        VNode[] res = new VNode[(int) num];
        long start = 0;
        for (int i = 0; i < num; i++) {
            long upperBound = start + rangeUnit + ((remaining-- <= 0) ? 0 : 1);
            res[i] = new VNode(start, upperBound);
            start = upperBound;
        }
        return res;
    }

    /**
     * @param size,  count of VNodes
     * @param ports, all ports
     * @return array of shuffled ports.
     */
    public static List<Integer> distributeVNodes(int size, Collection<Integer> ports) {
        List<Integer> shuffledPorts = new ArrayList<>(size);
        for (int i = 0; i < size / ports.size(); i++) {
            shuffledPorts.addAll(ports);
        }
        Collections.shuffle(shuffledPorts, new Random(1));
        return shuffledPorts;
    }

}
