package ru.mail.polis.service.vladislav_fetisov;

import one.nio.http.Request;
import one.nio.http.Response;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static ru.mail.polis.service.vladislav_fetisov.topology.Topology.TIMEOUT;


public final class HttpRequests {
    private HttpRequests() {

    }

    public static HttpRequest buildRequestFrom(Request request, String uri) {
        HttpRequest.Builder template = HttpRequest
                .newBuilder()
                .uri(URI.create(uri))
                .timeout(TIMEOUT);
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                return template
                        .GET()
                        .build();
            case Request.METHOD_PUT:
                return template
                        .PUT(HttpRequest.BodyPublishers.ofByteArray(request.getBody()))
                        .build();
            case Request.METHOD_DELETE:
                return template
                        .DELETE()
                        .build();
            default:
                throw new IllegalArgumentException();
        }
    }

    public static String getStatusFromCode(int code) {
        switch (code) {
            case 200:
                return Response.OK;
            case 201:
                return Response.CREATED;
            case 202:
                return Response.ACCEPTED;
            case 400:
                return Response.BAD_REQUEST;
            case 404:
                return Response.NOT_FOUND;
            case 503:
                return Response.SERVICE_UNAVAILABLE;
            case 504:
                return Response.GATEWAY_TIMEOUT;
            default:
                throw new IllegalArgumentException("Code not allowed " + code);
        }
    }

    public static List<HttpResponse<byte[]>> createEmptyList(int size) {
        List<HttpResponse<byte[]>> responses = new ArrayList<>(size);
        IntStream.range(0, size).forEach(i -> responses.add(null));
        return responses;
    }
}
