package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.util.Utf8;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

import static ru.mail.polis.service.vladislav_fetisov.MyService.logger;

public class Task implements Runnable {
    private static final byte[] MESSAGE_BODY = Utf8.toBytes("Не помещается в очередь");
    private final Runnable runnable;
    private final HttpSession session;

    public Task(Runnable runnable, HttpSession session) {
        this.runnable = runnable;
        this.session = session;
    }

    private void reject() throws IOException {
        session.sendResponse(new Response(Response.SERVICE_UNAVAILABLE, MESSAGE_BODY));
    }

    @Override
    public void run() {
        runnable.run();
    }

    public static class RejectedHandler implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable runnable, ThreadPoolExecutor threadPoolExecutor) {
            Task task = (Task) runnable;
            try {
                task.reject();
            } catch (IOException e) {
                logger.error("Failed to properly rejected task", e);
            }
        }
    }
}
