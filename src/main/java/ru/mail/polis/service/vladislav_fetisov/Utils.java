package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;
import one.nio.util.Hash;
import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;

import static ru.mail.polis.service.vladislav_fetisov.MyService.bytesLF;

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

    public static int writeEntry(int pos, Record record, byte[] res) {
        ByteBuffer key = record.getKey();
        key.get(res, pos, key.limit());
        pos += key.limit();
        pos = writeToArray(bytesLF, res, pos, bytesLF.length);
        ByteBuffer value = record.getValue();
        value.get(res, pos, value.limit());
        pos += value.limit();
        return pos;
    }

    public static int writeToArray(byte[] src, byte[] dst, int dstPos, int length) {
        System.arraycopy(src, 0, dst, dstPos, length);
        return dstPos + length;
    }
}
