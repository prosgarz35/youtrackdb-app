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
import org.apache.james.mailrepository.api.MailKey;
import org.apache.james.mailrepository.api.MailRepository;
import org.apache.james.mailrepository.api.MailRepositoryFactory;
import org.apache.james.mailrepository.api.MailRepositoryUrl;
import org.apache.james.mailrepository.blob.BlobMailRepositoryFactory;
import org.apache.mailet.Mail;

/**
 * Creates the blob mail repositories on top of the YouTrackDB blob store.
 *
 * <p>Temporary guard: {@code BlobMailRepository.removeAll()} lists every blob of the default bucket and removes
 * them all, not only the ones of its own repository. The repositories returned here remove the mails one by one,
 * from {@code list()}, which is filtered by repository. Drop this wrapper once removeAll() is fixed upstream.
 */
public class YouTrackDBBlobMailRepositoryFactory implements MailRepositoryFactory {
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
                String metaBlobPath = metadataPrefix() + key.asString();
                var bytesBlob = reactor.core.publisher.Mono.from(
                    blobStoreDAO.readBytes(defaultBucketName, new org.apache.james.blob.api.PlainBlobId(metaBlobPath))
                ).block();
                if (bytesBlob != null && bytesBlob.payload() != null && bytesBlob.payload().length > 0) {
                    com.fasterxml.jackson.databind.JsonNode root =
                        new com.fasterxml.jackson.databind.ObjectMapper().readTree(bytesBlob.payload());
                    if (root.has("headerBlobId") && root.get("headerBlobId").isTextual()) {
                        parts.add(new org.apache.james.blob.api.PlainBlobId(root.get("headerBlobId").asText()));
                    }
                    if (root.has("bodyBlobId") && root.get("bodyBlobId").isTextual()) {
                        parts.add(new org.apache.james.blob.api.PlainBlobId(root.get("bodyBlobId").asText()));
                    }
                }
            } catch (Exception ignored) {
                // If previous metadata doesn't exist or is not readable, no old parts to delete
            }
            return parts;
        }

        private void cleanOldMimeParts(List<BlobId> oldParts) {
            for (BlobId partId : oldParts) {
                try {
                    reactor.core.publisher.Mono.from(blobStoreDAO.delete(defaultBucketName, partId)).block();
                } catch (Exception ignored) {
                }
            }
        }

        @Override
        public Iterator<MailKey> list() throws MessagingException {
            if (blobStoreDAO instanceof YouTrackDBBlobStoreDAO ytdbDao) {
                return reactor.core.publisher.Flux.from(ytdbDao.listBlobs(defaultBucketName, metadataPrefix()))
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
