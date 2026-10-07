package org.apache.james.youtrackdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.mail.MessagingException;

import org.apache.james.blob.api.BlobId;
import org.apache.james.blob.api.BlobStore;
import org.apache.james.blob.api.BlobStoreDAO;
import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.ObjectNotFoundException;
import org.apache.james.blob.api.PlainBlobId;
import org.apache.james.mailrepository.api.MailKey;
import org.apache.james.mailrepository.api.MailRepository;
import org.apache.james.mailrepository.api.MailRepositoryFactory;
import org.apache.james.mailrepository.api.MailRepositoryUrl;
import org.apache.james.mailrepository.blob.BlobMailRepositoryFactory;
import org.apache.mailet.Mail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Creates the blob mail repositories on top of the YouTrackDB blob store.
 *
 * <p>Temporary guard: {@code BlobMailRepository.removeAll()} lists every blob of the default bucket and removes
 * them all, not only the ones of its own repository. The repositories returned here remove the mails one by one,
 * from {@code list()}, which is filtered by repository. Drop this wrapper once removeAll() is fixed upstream.
 */
public class YouTrackDBBlobMailRepositoryFactory implements MailRepositoryFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(YouTrackDBBlobMailRepositoryFactory.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final BlobMailRepositoryFactory delegate;
    private final BlobStoreDAO blobStoreDAO;
    private final BucketName defaultBucketName;

    @Inject
    public YouTrackDBBlobMailRepositoryFactory(BlobStoreDAO blobStoreDAO,
                                               BlobId.Factory blobIdFactory,
                                               @Named(BlobStore.DEFAULT_BUCKET_NAME_QUALIFIER) BucketName defaultBucketName) {
        this.delegate = new BlobMailRepositoryFactory(blobStoreDAO, blobIdFactory, defaultBucketName);
        this.blobStoreDAO = blobStoreDAO;
        this.defaultBucketName = defaultBucketName;
    }

    @Override
    public Class<? extends MailRepository> mailRepositoryClass() {
        return delegate.mailRepositoryClass();
    }

    @Override
    public MailRepository create(MailRepositoryUrl url) {
        return new RemoveAllByKey(delegate.create(url), url, blobStoreDAO, defaultBucketName);
    }

    private static class RemoveAllByKey implements MailRepository {
        private final MailRepository delegate;
        private final MailRepositoryUrl url;
        private final BlobStoreDAO blobStoreDAO;
        private final BucketName defaultBucketName;

        RemoveAllByKey(MailRepository delegate, MailRepositoryUrl url, BlobStoreDAO blobStoreDAO, BucketName defaultBucketName) {
            this.delegate = delegate;
            this.url = url;
            this.blobStoreDAO = blobStoreDAO;
            this.defaultBucketName = defaultBucketName;
        }

        private String metadataPrefix() {
            String path = url.subUrl("mailMetadata").getPath().asString();
            return path.endsWith("/") ? path : path + "/";
        }

        @Override
        public long size() throws MessagingException {
            if (blobStoreDAO instanceof YouTrackDBBlobStoreDAO ytdbDao) {
                Long count = ytdbDao.countBlobs(defaultBucketName, metadataPrefix()).block();
                return count != null ? count : 0L;
            }
            return delegate.size();
        }

        /**
         * The new mail is stored first, the parts of the previous version are deleted afterwards: a crash in
         * between leaves orphaned parts, never a lost mail.
         */
        @Override
        public MailKey store(Mail mail) throws MessagingException {
            MailKey key = MailKey.forMail(mail);
            List<BlobId> oldParts = findMimePartsForMailKey(key);
            MailKey storedKey = delegate.store(mail);
            if (!oldParts.isEmpty()) {
                cleanOldMimeParts(oldParts);
            }
            return storedKey;
        }

        private List<BlobId> findMimePartsForMailKey(MailKey key) {
            List<BlobId> parts = new ArrayList<>();
            try {
                var metadata = Mono.from(blobStoreDAO.readBytes(defaultBucketName, new PlainBlobId(metadataPrefix() + key.asString()))).block();
                if (metadata != null && metadata.payload() != null && metadata.payload().length > 0) {
                    JsonNode json = JSON.readTree(metadata.payload());
                    for (String field : List.of("headerBlobId", "bodyBlobId")) {
                        if (json.path(field).isTextual()) {
                            parts.add(new PlainBlobId(json.get(field).asText()));
                        }
                    }
                }
            } catch (ObjectNotFoundException e) {
                // First time this key is stored: nothing to clean up.
            } catch (Exception e) {
                LOGGER.warn("Cannot read the previous parts of mail {}: they will stay in the blob store", key.asString(), e);
            }
            return parts;
        }

        private void cleanOldMimeParts(List<BlobId> oldParts) {
            for (BlobId partId : oldParts) {
                try {
                    Mono.from(blobStoreDAO.delete(defaultBucketName, partId)).block();
                } catch (Exception e) {
                    LOGGER.warn("Cannot delete the replaced blob {}: it is now orphaned", partId.asString(), e);
                }
            }
        }

        @Override
        public Iterator<MailKey> list() throws MessagingException {
            if (blobStoreDAO instanceof YouTrackDBBlobStoreDAO ytdbDao) {
                return Flux.from(ytdbDao.listBlobs(defaultBucketName, metadataPrefix()))
                    .map(blobId -> new MailKey(blobId.asString()))
                    .toIterable()
                    .iterator();
            }
            return delegate.list();
        }


        @Override
        public Iterator<MailKey> list(Condition condition) throws MessagingException {
            return delegate.list(condition);
        }

        @Override
        public Mail retrieve(MailKey key) throws MessagingException {
            return delegate.retrieve(key);
        }

        @Override
        public void remove(MailKey key) throws MessagingException {
            delegate.remove(key);
        }

        @Override
        public void remove(Collection<MailKey> keys) throws MessagingException {
            delegate.remove(keys);
        }

        @Override
        public void removeAll() throws MessagingException {
            removeAll(key -> { });
        }

        @Override
        public void removeAll(Consumer<MailKey> progressCallback) throws MessagingException {
            List<MailKey> keys = new ArrayList<>();
            delegate.list().forEachRemaining(keys::add);
            for (MailKey key : keys) {
                delegate.remove(key);
                progressCallback.accept(key);
            }
        }
    }
}
