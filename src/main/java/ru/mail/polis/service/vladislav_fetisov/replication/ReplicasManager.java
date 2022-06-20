package ru.mail.polis.service.vladislav_fetisov.replication;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import ru.mail.polis.service.vladislav_fetisov.topology.Topology;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static ru.mail.polis.service.vladislav_fetisov.MyService.V_0_REPLICATION;
import static ru.mail.polis.service.vladislav_fetisov.MyService.logger;

public class ReplicasManager {
    private static final int TIMEOUT_MILLIS = 100;
    private static final ExecutorService replicaExecutor = Executors.newFixedThreadPool(8);
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

    public Response sendToAvailableReplica(Request request) throws InterruptedException {
        int[] sortedPorts = topology.getSortedPorts();
        int currentIndex = indexOfPort;
        int i = 0;
        while (true) {
            try {
                return topology.getClientByPort(sortedPorts[currentIndex]).invoke(request, TIMEOUT_MILLIS);
            } catch (PoolException | IOException | HttpException e) {
                logger.error("Response from partition on port: " + sortedPorts[currentIndex], e);
                if ((from - ++i) < ack) {
                    throw new NoEnoughReplicaAvailableException();
                }
                currentIndex++;
            }
        }
    }

    public static Request createReplicaRequest(Request request, String id) {
        String uri = V_0_REPLICATION + "?id=" + id;
        Request req = new Request(request.getMethod(), uri, false);
        byte[] body = request.getBody();
        if (body != null) {
            req.addHeader("Content-Length: " + body.length);
            req.setBody(body);
        }
        return req;
    }

    public Response[] processRequestWithReplication(Request request, String id) throws InterruptedException {
        ResponsesWithSync responsesWithSync = forwardRequestToReplicas(request, id);
        responsesWithSync.lock.lock();
        try {
            Response[] responses = responsesWithSync.responses;
            while (ack > 1 && responses[responses.length - 1] == null) {
                long remaining = responsesWithSync.condition.awaitNanos(TIMEOUT_MILLIS * 1_000_000L);
                if (remaining <= 0) {
                    return new Response[0];
                }
            }
        } finally {
            responsesWithSync.lock.unlock();
        }
        return responsesWithSync.responses;
    }

    private ResponsesWithSync forwardRequestToReplicas(Request request, String id) {
        Request req = ReplicasManager.createReplicaRequest(request, id);
        int[] sortedPorts = topology.getSortedPorts();

        AtomicInteger responseIndex = new AtomicInteger(1);
        Response[] responses = new Response[ack];
        ResponsesWithSync responsesWithSync = new ResponsesWithSync(responses);
        int index;
        for (int i = indexOfPort; i < indexOfPort + from; i++) {
            index = i % sortedPorts.length;
            if (index == indexOfOurPort) {
                continue;
            }
            HttpClient replicaClient = topology.getClientByPort(sortedPorts[index]);
            int finalIndex = index;
            replicaExecutor.execute(() -> {
                try {
                    Response response = replicaClient.invoke(req, TIMEOUT_MILLIS);
                    int ind = responseIndex.getAndIncrement();
                    if (ind >= responses.length) {
                        return;
                    }
                    responses[ind] = response;
                    if (ind == responses.length - 1) {
                        responsesWithSync.lock.lock();
                        try {
                            responsesWithSync.condition.signal();
                        } finally {
                            responsesWithSync.lock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("Request was cancelled");
                } catch (HttpException | IOException | PoolException e) {
                    logger.error("Response from replica on port " + sortedPorts[finalIndex], e);
                }
            });
        }
        return responsesWithSync;
    }

    private static class ResponsesWithSync {
        private final Response[] responses;
        private final Lock lock = new ReentrantLock();

        private final Condition condition = lock.newCondition();

        private ResponsesWithSync(Response[] responses) {
            this.responses = responses;
        }
    }
}