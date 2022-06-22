package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.*;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.vladislav_fetisov.replication.ReplicasManager;
import ru.mail.polis.service.vladislav_fetisov.topology.Topology;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MyService extends HttpServer implements Service {
    public static final String ID_PARAM = "?id=";
    public static final String V_0_REPLICATION = "/v0/replication";
    public static final String V_0_ENTITY = "/v0/entity";
    private static final String TIME_HEADER = "time";
    private static final int NOT_FOUND = 404;
    private static final long EXECUTOR_TIMEOUT = Topology.TIMEOUT.toMillis();
    private final DAO dao;
    private final Topology topology;
    private final ThreadPoolExecutor service = newThreadPool(8, port);
    public static final Logger logger = LoggerFactory.getLogger(MyService.class);

    public MyService(int port, DAO dao, Topology topology) throws IOException {
        super(Utils.from(port));
        this.dao = dao;
        this.topology = topology;
    }

    @Path("/v0/status")
    @RequestMethod(Request.METHOD_GET)
    public Response getStatus() {
        return Response.ok("All good");
    }

    @Path(V_0_ENTITY)
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

    private void replicaRequest(Request request, HttpSession session, String id, String replicas) throws IOException {
        try {
            getResponse(request, session, id, replicas);
        } catch (IllegalArgumentException e) {
            logger.error("", e);
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        } catch (Exception e) {
            logger.error("Internal error", e);
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Utf8.toBytes("Internal error")));
        }
    }

    private void getResponse(Request request, HttpSession session, String id, String replicas) throws IOException {
        if (id.isBlank()) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        int port = topology.findPort(Utils.getHash(id));
        int indexOfPort = topology.indexOfSortedPort(port);
        int indexOfOurPort = topology.indexOfSortedPort(this.port);
        ReplicasManager replicasManager = ReplicasManager.parseReplicas(replicas, topology, indexOfPort, indexOfOurPort);

        if (!replicasManager.currentNodeIsReplica()) {
            sendToAvailableReplica(request, id, session, replicasManager);
            return;
        }
        processRequestWithReplication(request, id, session, replicasManager);
    }

    public void sendToAvailableReplica(
            Request request, String id, HttpSession session, ReplicasManager replicasManager) {
        getAsyncReplicaResponse(request, id, session, replicasManager, replicasManager.getIndexOfPort(), 0);
    }

    private void getAsyncReplicaResponse(
            Request request, String id, HttpSession session, ReplicasManager replicasManager, int currentIndex, int i) {
        HttpRequest httpRequest = createRequest(request, id, V_0_ENTITY, currentIndex);
        topology.getClient()
                .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                .whenCompleteAsync((httpResponse, throwable) -> {
                    try {
                        if (throwable != null) {
                            logger.error("", throwable);
                            if ((replicasManager.getFrom() - (i + 1)) < replicasManager.getAck()) {
                                session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                                return;
                            }
                            getAsyncReplicaResponse(
                                    request, id, session, replicasManager, currentIndex + 1, i + 1);
                            return;
                        }
                        session.sendResponse(new Response
                                (HttpRequests.getStatusFromCode(httpResponse.statusCode()), httpResponse.body()));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private HttpRequest createRequest(Request request, String id, String endpoint, int currentIndex) {
        int[] sortedPorts = topology.getSortedPorts();
        int currentPort = sortedPorts[currentIndex % sortedPorts.length];
        String uri = (topology.getHostByPort(currentPort) + endpoint) + ID_PARAM + id;
        return HttpRequests.buildRequestFrom(request, uri);
    }

    public void processRequestWithReplication(
            Request request, String id, HttpSession session, ReplicasManager replicasManager) {
        Response localResponse = processRequest(request, id);
        AtomicBoolean timeout = forwardRequestToReplicas(request, id, session, replicasManager, localResponse);
        if (replicasManager.getAck() == 1) {
            sendResponse(request, localResponse, session, Collections.emptyList());
            return;
        }
        CompletableFuture
                .delayedExecutor(EXECUTOR_TIMEOUT, TimeUnit.MILLISECONDS)
                .execute(() -> {
                    if (!timeout.compareAndSet(false, true)) {
                        return;
                    }
                    try {
                        logger.error("Replicas dont reply");
                        session.sendResponse(new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private AtomicBoolean forwardRequestToReplicas(
            Request request, String id, HttpSession session, ReplicasManager replicasManager, Response localResponse) {

        AtomicInteger responseIndex = new AtomicInteger();
        AtomicBoolean timeout = new AtomicBoolean();
        List<HttpResponse<byte[]>> responses = HttpRequests.createEmptyList(replicasManager.getAck() - 1);


        int start = replicasManager.getIndexOfPort();
        for (int i = start; i < start + replicasManager.getFrom(); i++) {
            if (i % topology.getSortedPorts().length == replicasManager.getIndexOfOurPort()) {
                continue;
            }
            HttpRequest httpRequest = createRequest(request, id, V_0_REPLICATION, i);
            topology.getClient()
                    .sendAsync(httpRequest, HttpResponse.BodyHandlers.ofByteArray())
                    .whenCompleteAsync((httpResponse, throwable) -> {
                        if (throwable != null) {
                            logger.error("", throwable);
                            return;
                        }
                        int ind = responseIndex.getAndIncrement();
                        if (ind >= responses.size()) {
                            return;
                        }
                        responses.set(ind, httpResponse);
                        if (ind != responses.size() - 1 || !timeout.compareAndSet(false, true)) {
                            return;
                        }
                        sendResponse(request, localResponse, session, responses);
                    });
        }
        return timeout;
    }

    private void sendResponse(
            Request request, Response localResponse, HttpSession session, List<HttpResponse<byte[]>> responses) {
        try {
            switch (request.getMethod()) {
                case Request.METHOD_DELETE:
                    session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
                    return;
                case Request.METHOD_GET:
                    processGet(session, responses, localResponse);
                    return;
                default:
                    session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private void processGet(
            HttpSession session, List<HttpResponse<byte[]>> responses, Response ourResponse) throws IOException {
        long newestTime = -1;
        HttpResponse<byte[]> newestResp = null;
        boolean isHttpResponse = true;
        for (HttpResponse<byte[]> response : responses) {
            OptionalLong optParameter = response.headers().firstValueAsLong(TIME_HEADER);
            if (optParameter.isEmpty()) {
                continue;
            }
            long time = optParameter.getAsLong();
            if (time > newestTime) {
                newestResp = response;
                newestTime = time;
            }
        }
        String timeHeader = ourResponse.getHeader(TIME_HEADER + ":");
        if (timeHeader != null && Long.parseLong(timeHeader) > newestTime) {
            isHttpResponse = false;
        }
        if (isHttpResponse) {
            if (newestResp == null || newestResp.statusCode() == NOT_FOUND) {
                session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
                return;
            }
            session.sendResponse(new Response(Response.OK, newestResp.body()));
            return;
        }
        if (ourResponse.getStatus() == NOT_FOUND) {
            session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
            return;
        }
        session.sendResponse(new Response(Response.OK, ourResponse.getBody()));
    }

    public Response processRequest(Request request, String id) {
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
            response.addHeader(String.format(TIME_HEADER + ":%d", record.getCreatedTime()));
            return response;
        }
        Response response = new Response(Response.OK, Utils.from(record.getValue()));
        response.addHeader(String.format(TIME_HEADER + ":%d", record.getCreatedTime()));
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

