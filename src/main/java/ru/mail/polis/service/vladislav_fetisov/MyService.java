package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.Record;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.*;

public class MyService extends HttpServer implements Service {
    public static final String STATUS_PATH = "/v0/status";
    public static final String ENTITY_PATH = "/v0/entity";
    private final DAO dao;
    private final ThreadPoolExecutor service = newThreadPool();
    public static final Logger logger = LoggerFactory.getLogger(MyService.class);

    public MyService(int port, DAO dao) throws IOException {
        super(from(port));
        this.dao = dao;
    }

    public Response getStatus() {
        return Response.ok("");
    }

    public Response entity(Request request) {
        String id = request.getParameter("id=");
        if (id == null || id.isBlank()) {
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
    public void handleRequest(Request request, HttpSession session) throws IOException {
        this.handleDefault(request, session);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Runnable runnable = () -> {
            try {
                Response response = handleRequest(request);
                session.sendResponse(response);
            } catch (IOException e) {
                session.handleException(e);
            }
        };
        service.execute(new Task(runnable, session));
    }

    private Response handleRequest(Request request) {
        switch (request.getPath()) {
            case STATUS_PATH:
                return getStatus();
            case ENTITY_PATH:
                return entity(request);
            default:
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
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

    private static ThreadPoolExecutor newThreadPool() {
        return new ThreadPoolExecutor(4,
                4,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1024),
                new Task.RejectedHandler());
    }

    public static void main(String[] args) {
        Runnable runnable = () -> {
            System.out.println(1);
        };
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(4,
                4,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(2),
                new ThreadPoolExecutor.AbortPolicy());
        for (int i = 0; i < 10; i++) {
            threadPoolExecutor.execute(runnable);
        }
        threadPoolExecutor.shutdown();
    }
}
