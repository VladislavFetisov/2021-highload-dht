package ru.mail.polis.service.vladislav_fetisov.topology;

import one.nio.http.HttpClient;
import one.nio.net.ConnectionString;
import ru.mail.polis.service.vladislav_fetisov.Utils;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Topology {
    private static final Duration TIMEOUT = Duration.ofSeconds(1);
    private static final int VNODES_COUNT = 8;
    private final VNode[] vNodes;
    private final Map<Integer, HttpClient> portsToClients;
    private final int[] sortedPorts;

    public int[] getSortedPorts() {
        return sortedPorts;
    }

    private final List<Integer> shuffledPorts;

    public Topology(Set<String> endpoints, Range range) {
        vNodes = VNode.getAllVNodes(endpoints.size(), VNODES_COUNT, range);
        portsToClients = portsToClients(endpoints);
        shuffledPorts = VNode.distributeVNodes(vNodes.length, portsToClients.keySet());
        List<Integer> list = portsToClients
                .keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList());
        sortedPorts = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            sortedPorts[i] = list.get(i);
        }
    }

    private static Map<Integer, HttpClient> portsToClients(Set<String> topology) {
        Map<Integer, HttpClient> res = new HashMap<>(topology.size());
        for (String endpoint : topology) {
            res.put(Utils.getPort(endpoint),
                    new HttpClient(new ConnectionString(endpoint + "?timeout=" + TIMEOUT.toMillis())));
        }
        return res;
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

    public HttpClient getClientByPort(int port) {
        return portsToClients.get(port);
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
}