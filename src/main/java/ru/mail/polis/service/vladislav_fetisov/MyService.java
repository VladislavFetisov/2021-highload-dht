package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.*;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.vladislav_fetisov.replication.NoEnoughReplicaAvailableException;
import ru.mail.polis.service.vladislav_fetisov.replication.ReplicasManager;
import ru.mail.polis.service.vladislav_fetisov.topology.Topology;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyService extends HttpServer implements Service {
    public static final String V_0_REPLICATION = "/v0/replication";
    private static final String TIME_HEADER = "Time: ";
    private static final int NOT_FOUND = 404;
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
        } catch (NoEnoughReplicaAvailableException e) {
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
        ReplicasManager replicasManager = ReplicasManager.parseReplicas(replicas, topology);

        int port = topology.findPort(Utils.getHash(id));
        int indexOfPort = topology.indexOfSortedPort(port);
        int indexOfOurPort = topology.indexOfSortedPort(this.port);

        if (!replicasManager.currentNodeIsReplica(indexOfPort, indexOfOurPort)) {
            return replicasManager.sendToAvailableReplica(request, indexOfPort);
        }
        Response[] responses = replicasManager.processRequestWithReplication(request, id, indexOfPort, indexOfOurPort);
        if (responses.length == 0) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
        responses[0] = processRequest(request, id);
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
