package org.apache.james.youtrackdb;

import java.util.Objects;
import java.util.UUID;

import org.apache.james.mailbox.model.MessageId;

public final class YouTrackDBMessageId implements MessageId {

    public static final class Factory implements MessageId.Factory {
        @Override
        public MessageId fromString(String serialized) {
            return YouTrackDBMessageId.of(serialized);
        }

        @Override
        public MessageId generate() {
            return YouTrackDBMessageId.generate();
        }
    }

    private final String id;

    private YouTrackDBMessageId(String id) {
        this.id = Objects.requireNonNull(id, "id must not be null");
    }

    public static YouTrackDBMessageId generate() {
        return new YouTrackDBMessageId(UUID.randomUUID().toString());
    }

    public static YouTrackDBMessageId of(String id) {
        return new YouTrackDBMessageId(id);
    }

    @Override
    public String serialize() {
        return id;
    }

    @Override
    public boolean isSerializable() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        YouTrackDBMessageId that = (YouTrackDBMessageId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return id;
    }
}
