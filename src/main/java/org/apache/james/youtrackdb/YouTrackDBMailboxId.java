package org.apache.james.youtrackdb;

import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

import org.apache.james.mailbox.model.MailboxId;

public final class YouTrackDBMailboxId implements MailboxId {

    public static final class Factory implements MailboxId.Factory {
        @Override
        public MailboxId fromString(String serialized) {
            return YouTrackDBMailboxId.of(serialized);
        }
    }

    private final String id;

    private YouTrackDBMailboxId(String id) {
        Objects.requireNonNull(id, "id must not be null");
        UUID.fromString(id);
        this.id = id.toUpperCase(Locale.US);
    }

    public static YouTrackDBMailboxId generate() {
        return new YouTrackDBMailboxId(UUID.randomUUID().toString());
    }

    public static YouTrackDBMailboxId of(String id) {
        return new YouTrackDBMailboxId(id);
    }

    @Override
    public String serialize() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        YouTrackDBMailboxId that = (YouTrackDBMailboxId) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    @Override
    public String toString() {
        return id;
    }
}
