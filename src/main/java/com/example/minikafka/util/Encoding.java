package com.example.minikafka.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class Encoding {
    private Encoding() {}

    public static String b64(String value) {
        if (value == null) {
            value = "";
        }
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    public static String fromB64(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return "";
        }
        return new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8);
    }
}
