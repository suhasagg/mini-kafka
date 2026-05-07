package com.example.minikafka.util;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

public final class QueryString {
    private QueryString() {}

    public static Map<String, String> parse(URI uri) {
        Map<String, String> result = new LinkedHashMap<>();
        String query = uri.getRawQuery();
        if (query == null || query.isEmpty()) {
            return result;
        }
        for (String part : query.split("&")) {
            if (part.isEmpty()) {
                continue;
            }
            String[] kv = part.split("=", 2);
            String key = decode(kv[0]);
            String value = kv.length == 2 ? decode(kv[1]) : "";
            result.put(key, value);
        }
        return result;
    }

    private static String decode(String s) {
        try {
            return URLDecoder.decode(s, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }
}
