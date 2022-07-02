package ru.mail.polis.service.vladislav_fetisov.topology;

import ru.mail.polis.service.vladislav_fetisov.Utils;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public class Topology {
    private static final int VNODES_COUNT = 8;
    public static final Duration TIMEOUT = Duration.ofMillis(500);
    private final VNode[] vNodes;
    private final Map<Integer, String> portsToHosts;
    private final java.net.http.HttpClient client;
    private final int[] sortedPorts;

    private final List<Integer> shuffledPorts;

    public Topology(Set<String> endpoints, Range range) {
        vNodes = VNode.getAllVNodes(endpoints.size(), VNODES_COUNT, range);
        portsToHosts = portsToHosts(endpoints);
        shuffledPorts = VNode.distributeVNodes(vNodes.length, portsToHosts.keySet());
        List<Integer> list = portsToHosts
                .keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList());
        sortedPorts = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            sortedPorts[i] = list.get(i);
        }
        ForkJoinPool pool = new ForkJoinPool();
        client = java.net.http.HttpClient
                .newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(TIMEOUT)
                .executor(pool)
                .build();
    }

    private static Map<Integer, String> portsToHosts(Set<String> topology) {
        return topology
                .stream()
                .collect(Collectors.toMap(Utils::getPort, s -> s, (a, b) -> b, () -> new HashMap<>(topology.size())));

    }

    public int findPort(long idHashCode) {
        int l = binarySearch(vNodes, idHashCode);
        if (l == vNodes.length) {
            throw new IllegalStateException(String.format("Hash: %d больше верхней границы: %d",
                    idHashCode, vNodes[vNodes.length - 1].upperBound));
        }
        if (l == -1) {
            throw new IllegalStateException(String.format("Hash %d не принадлежит ни одной VNode", idHashCode));
        }
        return shuffledPorts.get(l);
    }

    public String getHostByPort(int port) {
        return portsToHosts.get(port);
    }

    public int indexOfSortedPort(int port) {
        return Arrays.binarySearch(sortedPorts, port);
    }

    private static int binarySearch(VNode[] vNodes, long hash) {
        int l = 0;
        int r = vNodes.length - 1;
        while (l <= r) {
            int mid = (l + r) >>> 1;
            int res = compareVNodes(hash, vNodes[mid]);
            if (res == 0) {
                return mid;
            } else if (res > 0) {
                l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        return -1;
    }

    private static int compareVNodes(long hash, VNode vNode) {
        if (hash >= vNode.lowBound && hash <= vNode.upperBound) {
            return 0;
        }
        if (hash > vNode.upperBound) {
            return 1;
        }
        return -1;
    }

    public int getQuorum() {
        return (sortedPorts.length / 2) + 1;
    }


    public int[] getSortedPorts() {
        return sortedPorts;
    }

    public java.net.http.HttpClient getClient() {
        return client;
    }
}
