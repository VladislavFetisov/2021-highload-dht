package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.*;
import one.nio.pool.PoolException;
import one.nio.util.Hash;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.vladislav_fetisov.replication.PartitionNotAvailableException;
import ru.mail.polis.service.vladislav_fetisov.replication.Replicas;
import ru.mail.polis.service.vladislav_fetisov.topology.Topology;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MyService extends HttpServer implements Service {
    private static final int TIMEOUT_MILLIS = 20000;
    public static final String V_0_REPLICATION = "/v0/replication";
    private static final String TIME_HEADER = "Time: ";
    private static final int NOT_FOUND = 404;
    private final DAO dao;
    private final Topology topology;
    private final ThreadPoolExecutor service;
    private final ExecutorService replicaExecutor = Executors.newFixedThreadPool(8);
    public static final Logger logger = LoggerFactory.getLogger(MyService.class);

    public MyService(int port, DAO dao, Topology topology) throws IOException {
        super(Utils.from(port));
        this.dao = dao;
        this.topology = topology;
        service = newThreadPool(8, port);
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response getStatus() {
        return Response.ok("All good");
    }

    @Path("/v0/entity")
    public void entity(
            Request request,
            HttpSession session,
            @Param(value = "id", required = true) String id,
            @Param(value = "replicas") String replicas) {
        Runnable runnable = () -> {
            try {
                replicaRequest(request, session, id, replicas);
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        };
        service.execute(new Task(runnable, session));
    }

    private void replicaRequest(Request request, HttpSession session, String id, String replicas) throws IOException {
        try {
            Response response = getResponse(request, id, replicas);
            session.sendResponse(response);
        } catch (InterruptedException e) {
            logger.error("", e);
            Thread.currentThread().interrupt();
        } catch (IllegalArgumentException e) {
            logger.error("", e);
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        } catch (PartitionNotAvailableException e) {
            logger.error("", e);
            session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
        } catch (Exception e) {
            logger.error("Internal error", e);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Utf8.toBytes("Internal error")));
        }
    }

    @Path(V_0_REPLICATION)
    public void replica(
            Request request,
            HttpSession session,
            @Param(value = "id", required = true) String id) {
        Runnable runnable = () -> {
            try {
                Response response = processRequest(request, id);
                session.sendResponse(response);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        service.execute(new Task(runnable, session));
    }

    private Response getResponse(Request request, String id, String replicas) throws InterruptedException {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        int hash = getHash(id);
        int port = topology.findPort(hash);
        if (port != this.port) {
            try {
                return topology.getClientByPort(port).invoke(request, TIMEOUT_MILLIS);
            } catch (PoolException | IOException | HttpException e) {
                logger.error("Response from partition on port: " + port, e);
                throw new PartitionNotAvailableException();
            }
        }
        Response[] responses = processRequestWithReplication(request, id, replicas);
        if (responses.length == 0) {
            return new Response(Response.GATEWAY_TIMEOUT);
        }

        switch (request.getMethod()) {
            case Request.METHOD_DELETE:
                return new Response(Response.ACCEPTED, Response.EMPTY);
            case Request.METHOD_PUT:
                return new Response(Response.CREATED, Response.EMPTY);
            case Request.METHOD_GET:
                return processGet(responses);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED);
        }

    }

    private Response processGet(Response[] responses) {
        Response newestResp = null;
        long newestTime = -1;
        for (Response response : responses) {
            String header = response.getHeader(TIME_HEADER);
            if (header == null) {
                continue;
            }
            long time = Long.parseLong(header);
            if (time > newestTime) {
                newestResp = response;
            }
        }
        if (newestResp == null || newestResp.getStatus() == NOT_FOUND) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        return new Response(Response.OK, newestResp.getBody());
    }

    private Response[] processRequestWithReplication(
            Request request, String id, String replicas) throws InterruptedException {
        int ack;
        int from;
        if (replicas == null) {
            ack = topology.getQuorum();
            from = topology.getSortedPorts().length;
        } else {
            Replicas parsedReplicas = Replicas.parseReplicas(replicas, topology);
            ack = parsedReplicas.getAck();
            from = parsedReplicas.getFrom();
        }
        ResponsesWithSync responsesWithSync = forwardRequestToReplicas(request, id, ack, from);
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

    private ResponsesWithSync forwardRequestToReplicas(Request request, String id, int ack, int from) {
        int[] sortedPorts = topology.getSortedPorts();
        int index = Arrays.binarySearch(sortedPorts, this.port);

        String uri = V_0_REPLICATION + "?id=" + id; //TODO move into method
        Request req = new Request(request.getMethod(), uri, false);
        byte[] body = request.getBody();
        if (body != null) {
            req.addHeader("Content-Length: " + body.length);
            req.setBody(body);
        }

        AtomicInteger responseIndex = new AtomicInteger(1);
        Response[] responses = new Response[ack];
        ResponsesWithSync responsesWithSync = new ResponsesWithSync(responses);
        for (int i = index + 1; i < index + 1 + from - 1; i++) {
            int replicaPort = sortedPorts[i % sortedPorts.length];
            HttpClient replicaClient = topology.getClientByPort(replicaPort);
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
                    logger.error("Response from replica " + replicaClient, e);
                }
            });
        }
        responses[0] = processRequest(request, id);
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

    private Response processRequest(Request request, String id) {
        logger.info("Request method {}, uri {}, body{}", request.getMethodName(), request.getURI(), request.getBody());
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id);
            case Request.METHOD_DELETE:
                return delete(id);
            case Request.METHOD_PUT:
                return put(id, request.getBody());
            default:
                return new Response(Response.METHOD_NOT_ALLOWED);
        }
    }

    private int getHash(String id) {
        int hash = Hash.murmur3(id);
        if (hash == Integer.MIN_VALUE) {
            return 0;
        }
        return Math.abs(hash);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    private Response put(String id, byte[] body) {
        boolean success = dao.upsert(Record.of(ByteBuffer.wrap(Utf8.toBytes(id)), ByteBuffer.wrap(body)));
        if (success) {
            return new Response(Response.CREATED, Response.EMPTY);
        }
        return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
    }

    private Response delete(String id) {
        boolean success = dao.upsert(Record.tombstone(ByteBuffer.wrap(Utf8.toBytes(id))));
        if (success) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
    }

    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        Record record = dao.get(key);
        if (record == null) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
        if (record.isTombstone()) {
            Response response = new Response(Response.NOT_FOUND, Response.EMPTY);
            response.addHeader(String.format(TIME_HEADER + "%d", record.getCreatedTime()));
            return response;
        }
        Response response = new Response(Response.OK, Utils.from(record.getValue()));
        response.addHeader(String.format(TIME_HEADER + "%d", record.getCreatedTime()));
        return response;
    }

    private static ThreadPoolExecutor newThreadPool(int cores, int port) {
        return new ThreadPoolExecutor(cores,
                cores,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                r -> new Thread(r, "Requests processor on port:" + port),
                new Task.RejectedHandler());
    }

}
