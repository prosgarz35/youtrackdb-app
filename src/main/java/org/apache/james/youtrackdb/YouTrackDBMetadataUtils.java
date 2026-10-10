package org.apache.james.youtrackdb;

import java.io.IOException;
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
        if (payload == null || payload.length == 0) {
            return List.of();
        }
        JsonNode json = JSON.readTree(payload);
        return java.util.stream.Stream.of("headerBlobId", "bodyBlobId")
            .map(json::path)
            .filter(JsonNode::isTextual)
            .map(JsonNode::asText)
            .toList();
    }
}
