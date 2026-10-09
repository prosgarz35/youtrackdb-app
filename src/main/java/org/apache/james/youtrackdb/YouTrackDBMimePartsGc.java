package org.apache.james.youtrackdb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import org.apache.james.blob.api.BlobId;
import org.apache.james.blob.api.BlobStore;
import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.ObjectNotFoundException;
import org.apache.james.blob.api.PlainBlobId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Collects the MIME parts (header and body blobs) of mails of the blob mail repositories that no mail refers to
 * any more: what is left when a process dies between the deletion of a mail's metadata and the deletion of its parts.
 *
 * <p>A part is referenced when the metadata of a mail ({@code <repository>/mailMetadata/<mail>}) names it as
 * {@code headerBlobId} or {@code bodyBlobId}. Blobs are not timestamped, and BlobMailRepository saves the parts
 * before the metadata, so a part can look orphaned for a few milliseconds while its mail is being stored. To be
 * safe a part is deleted only when two runs, one after the other, both find it unreferenced: the first run only
 * remembers it. Run it twice, a few minutes apart. The memory of the first run is lost on restart, which only
 * delays the deletion.
 *
 * <p>If one metadata blob cannot be read, nothing is deleted: the references are not known.
 */
@Singleton
public class YouTrackDBMimePartsGc {
    private static final Logger LOGGER = LoggerFactory.getLogger(YouTrackDBMimePartsGc.class);
    private static final Pattern METADATA = Pattern.compile("^.*/mailMetadata/[^/]+$");
    private static final Pattern PART = Pattern.compile("^.*/mimeMessagedata/[^/]+$");

    public record Result(long referencedParts, long deletedParts, long pendingParts) {
    }

    public static final java.time.Duration DEFAULT_MIN_AGE = java.time.Duration.ofMinutes(10);

    private final YouTrackDBBlobStoreDAO blobStoreDAO;
    private final BucketName bucketName;
    private final java.time.Duration minAge;
    private java.util.Map<String, java.time.Instant> candidates = new java.util.HashMap<>();

    @Inject
    public YouTrackDBMimePartsGc(YouTrackDBBlobStoreDAO blobStoreDAO,
                                 @Named(BlobStore.DEFAULT_BUCKET_NAME_QUALIFIER) BucketName bucketName) {
        this(blobStoreDAO, bucketName, DEFAULT_MIN_AGE);
    }

    public YouTrackDBMimePartsGc(YouTrackDBBlobStoreDAO blobStoreDAO,
                                 BucketName bucketName,
                                 java.time.Duration minAge) {
        this.blobStoreDAO = blobStoreDAO;
        this.bucketName = bucketName;
        this.minAge = minAge;
    }

    public synchronized Result collect() {
        List<String> metadataIds = new ArrayList<>();
        List<String> partIds = new ArrayList<>();
        Flux.from(blobStoreDAO.listBlobs(bucketName)).map(BlobId::asString).toIterable().forEach(id -> {
            if (METADATA.matcher(id).matches()) {
                metadataIds.add(id);
            } else if (PART.matcher(id).matches()) {
                partIds.add(id);
            }
        });

        Set<String> referenced = new HashSet<>();
        for (String metadataId : metadataIds) {
            referenced.addAll(referencedParts(metadataId));
        }

        Set<String> orphans = new HashSet<>(partIds);
        orphans.removeAll(referenced);

        java.time.Instant now = java.time.Instant.now();
        java.util.Map<String, java.time.Instant> stillOrphans = new java.util.HashMap<>();
        long deleted = 0;
        for (String orphan : orphans) {
            java.time.Instant firstSeen = candidates.get(orphan);
            if (firstSeen == null) {
                stillOrphans.put(orphan, now);
                continue;
            }
            if (java.time.Duration.between(firstSeen, now).compareTo(minAge) < 0) {
                stillOrphans.put(orphan, firstSeen);
                continue;
            }
            try {
                Mono.from(blobStoreDAO.delete(bucketName, new PlainBlobId(orphan))).block();
                deleted++;
            } catch (RuntimeException e) {
                LOGGER.warn("Cannot delete the orphaned MIME part {}", orphan, e);
                stillOrphans.put(orphan, firstSeen);
            }
        }
        candidates = stillOrphans;
        return new Result(referenced.size(), deleted, stillOrphans.size());
    }

    /** Empty when the metadata disappeared since the listing (its mail was removed). Fails when it cannot be read. */
    private Set<String> referencedParts(String metadataId) {
        try {
            var metadata = Mono.from(blobStoreDAO.readBytes(bucketName, new PlainBlobId(metadataId))).block();
            if (metadata == null || metadata.payload() == null) {
                return Set.of();
            }
            return new HashSet<>(YouTrackDBMetadataUtils.extractReferencedPartIds(metadata.payload()));
        } catch (ObjectNotFoundException e) {
            return Set.of();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot read " + metadataId + ": the references are unknown, nothing is deleted", e);
        }
    }
}
