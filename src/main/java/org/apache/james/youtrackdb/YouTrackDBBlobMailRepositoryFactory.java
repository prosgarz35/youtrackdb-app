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

    @Inject
    public YouTrackDBBlobMailRepositoryFactory(BlobStoreDAO blobStoreDAO,
                                               BlobId.Factory blobIdFactory,
                                               @Named(BlobStore.DEFAULT_BUCKET_NAME_QUALIFIER) BucketName defaultBucketName) {
        this.delegate = new BlobMailRepositoryFactory(blobStoreDAO, blobIdFactory, defaultBucketName);
    }

    @Override
    public Class<? extends MailRepository> mailRepositoryClass() {
        return delegate.mailRepositoryClass();
    }

    @Override
    public MailRepository create(MailRepositoryUrl url) {
        return new RemoveAllByKey(delegate.create(url));
    }

    private static class RemoveAllByKey implements MailRepository {
        private final MailRepository delegate;

        RemoveAllByKey(MailRepository delegate) {
            this.delegate = delegate;
        }

        @Override
        public long size() throws MessagingException {
            return delegate.size();
        }

        @Override
        public MailKey store(Mail mail) throws MessagingException {
            return delegate.store(mail);
        }

        @Override
        public Iterator<MailKey> list() throws MessagingException {
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
