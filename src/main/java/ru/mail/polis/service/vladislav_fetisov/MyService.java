package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyService extends HttpServer implements Service {
    private final DAO dao;
    private final ExecutorService service = Executors.newFixedThreadPool(4);

    public MyService(int port, DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
    }

    @Path("/v0/status")
    public Response getStatus() {
        return Response.ok("");
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Path("/v0/entity")
    public Response entity(
            Request request,
            @Param(value = "id", required = true) String id) {
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

    private Response put(String id, byte[] body) {
        dao.upsert(Record.of(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8)
        ), ByteBuffer.wrap(body)));
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(String id) {
        dao.upsert(Record.tombstone(ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8))));
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    private Response get(String id) {
        ByteBuffer key = ByteBuffer.wrap(id.getBytes(StandardCharsets.UTF_8));
        Iterator<Record> range = dao.range(key, DAO.nextKey(key));

        return (range.hasNext()) ? new Response(Response.OK, from(range.next().getValue())) :
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
        config.minWorkers = 4;
        config.maxWorkers = 4;
        return config;
    }

}
