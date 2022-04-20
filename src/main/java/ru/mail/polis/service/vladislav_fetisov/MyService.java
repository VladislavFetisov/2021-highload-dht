package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import one.nio.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyService extends HttpServer implements Service {
    private final DAO dao;
    private final ThreadPoolExecutor service = newThreadPool(4);
    public static final Logger logger = LoggerFactory.getLogger(MyService.class);

    public MyService(int port, DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
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
                handleException(session, e);
            }
        };
        service.execute(new Task(runnable, session));
    }

    private void handleException(HttpSession session, Exception e) {
        try {
            session.sendResponse(new Response(Response.INTERNAL_ERROR, Utf8.toBytes("Internal error")));
            logger.error("Internal error", e);
        } catch (IOException ex) {
            logger.error("Failed to send response", ex);
        }
    }

    private Response getResponse(Request request, String id) {
        if (id.isBlank()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
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
        boolean success = dao.upsert(Record.of(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap(body)));
        if (success) {
            return new Response(Response.CREATED, Response.EMPTY);
        }
        return new Response(Response.SERVICE_UNAVAILABLE, Response.EMPTY);
    }

    private Response delete(String id) {
        dao.upsert(Record.tombstone(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8))));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> range = dao.range(key, DAO.nextKey(key));

        return range.hasNext() ? new Response(Response.OK, from(range.next().getValue())) :
                new Response(Response.NOT_FOUND, Response.EMPTY);
    }

    private static byte[] from(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static HttpServerConfig from(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        ac.reusePort = true;
        config.acceptors = new AcceptorConfig[]{ac};
        config.selectors = 4;
        return config;
    }

    private static ThreadPoolExecutor newThreadPool(int cores) {
        return new ThreadPoolExecutor(cores,
                cores,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                new Task.RejectedHandler());
    }

}
