package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;

import java.io.IOException;
import java.util.function.Supplier;

public class MyHttpSession extends HttpSession {

    private Supplier<byte[]> supplier;

    public MyHttpSession(Socket socket, MyService service) {
        super(socket, service);
    }

    public void sendResponseWithSupplier(Response response, Supplier<byte[]> supplier) throws IOException {
        this.supplier = supplier;
        response.setBody(supplier.get());
        super.sendResponse(response);
        processChain();
    }

    private void processChain() throws IOException {
        while (queueHead == null) {
            byte[] bytes = supplier.get();
            if (bytes.length == 0) {
                super.scheduleClose();
                return;
            }
            write(bytes, 0, bytes.length);
        }
    }
}