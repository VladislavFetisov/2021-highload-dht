package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.*;
import one.nio.pool.PoolException;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.vladislav_fetisov.topology.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyService extends HttpServer implements Service {
    private final DAO dao;
    private final Topology topology;
    private final ThreadPoolExecutor service = newThreadPool(4);
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
            @Param(value = "id", required = true) String id) {
        Runnable runnable = () -> {
            try {
                Response response = getResponse(request, id);
                session.sendResponse(response);
            } catch (Exception e) {
                if (e.getClass().equals(InterruptedException.class)) {
                    Thread.currentThread().interrupt();
                }
                handleException(session, e);
            }
        };
        service.execute(new Task(runnable, session));
    }


    private Response getResponse(Request request, String id)
            throws HttpException, IOException, PoolException, InterruptedException {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        int hash = getHash(id);
        int port = topology.findPort(hash);
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return get(id, port, request);
            case Request.METHOD_DELETE:
                return delete(id, port, request);
            case Request.METHOD_PUT:
                return put(id, port, request);
            default:
                return new Response(Response.METHOD_NOT_ALLOWED);
        }
    }

    private int getHash(String id) {
        return id.hashCode() & 0xfffffff;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    private Response put(String id, int port, Request request)
            throws HttpException, IOException, PoolException, InterruptedException {
        if (port == this.port) {
            return put(id, request.getBody());
        }
        return topology
                .getClientByPort(port)
                .put(request.getURI(), request.getBody());
    }

    private Response put(String id, byte[] body) {
        boolean success = dao.upsert(Record.of(ByteBuffer.wrap(Utf8.toBytes(id)), ByteBuffer.wrap(body)));
        if (success) {
            return new Response(Response.CREATED, Response.EMPTY);
        }
        return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
    }

    private Response delete(String id, int port, Request request)
            throws HttpException, IOException, PoolException, InterruptedException {
        if (port == this.port) {
            return delete(id);
        }
        return topology
                .getClientByPort(port)
                .delete(request.getURI());
    }

    private Response delete(String id) {
        boolean success = dao.upsert(Record.tombstone(ByteBuffer.wrap(Utf8.toBytes(id))));
        if (success) {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
        return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
    }

    private Response get(String id, int port, Request request)
            throws HttpException, IOException, PoolException, InterruptedException {
        if (port == this.port) {
            return get(id);
        }
        return topology
                .getClientByPort(port)
                .get(request.getURI());
    }

    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(Utf8.toBytes(id));
        Iterator<Record> range = dao.range(key, DAO.nextKey(key));

        return range.hasNext() ? new Response(Response.OK, Utils.from(range.next().getValue())) :
                new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private static ThreadPoolExecutor newThreadPool(int cores) {
        return new ThreadPoolExecutor(cores,
                cores,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                new Task.RejectedHandler());
    }

    public static void handleException(HttpSession session, Exception e) {
        try {
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Utf8.toBytes("Internal error")));
            logger.error("Internal error", e);
        } catch (IOException ex) {
            logger.error("Failed to send response", ex);
        }
    }
}
