package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;
import one.nio.util.Hash;

import java.nio.ByteBuffer;

public final class Utils {

    private Utils() {
    }

    public static byte[] from(ByteBuffer buffer) {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public static HttpServerConfig from(int port) {
        HttpServerConfig config = new HttpServerConfig();
        AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        ac.reusePort = true;
        config.acceptors = new AcceptorConfig[]{ac};
        config.selectors = 4;
        return config;
    }

    public static int getPort(String url) {
        int i = url.lastIndexOf(':');
        return Integer.parseInt(url.substring(i + 1));
    }

    public static int getHash(String id) {
        int hash = Hash.murmur3(id);
        if (hash == Integer.MIN_VALUE) {
            return 0;
        }
        return Math.abs(hash);
    }

}
