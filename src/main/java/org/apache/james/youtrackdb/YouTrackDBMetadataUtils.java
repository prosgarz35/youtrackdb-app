package org.apache.james.youtrackdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility to extract MIME part blob ids from mail metadata payloads.
 */
public final class YouTrackDBMetadataUtils {

    private static final ObjectMapper JSON = new ObjectMapper();

    private YouTrackDBMetadataUtils() {
    }

    public static List<String> extractReferencedPartIds(byte[] payload) throws IOException {
        List<String> parts = new ArrayList<>();
        if (payload == null || payload.length == 0) {
            return parts;
        }
        JsonNode json = JSON.readTree(payload);
        for (String field : List.of("headerBlobId", "bodyBlobId")) {
            if (json.path(field).isTextual()) {
                parts.add(json.get(field).asText());
            }
        }
        return parts;
    }
}
