package org.apache.james.youtrackdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.james.core.MailAddress;
import org.apache.james.lifecycle.api.LifecycleUtil;
import org.apache.james.queue.api.MailQueue;
import org.apache.james.queue.api.MailQueueFactory;
import org.apache.james.queue.api.MailQueueItemDecoratorFactory;
import org.apache.james.queue.api.MailQueueName;
import org.apache.james.queue.api.ManageableMailQueue;
import org.apache.james.server.core.MailImpl;
import org.apache.mailet.Mail;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.fge.lambdas.Throwing;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Persistent MailQueue backed by YouTrackDB (at-least-once delivery):
 * - Every enqueued mail is committed to YouTrackDB (JamesQueueItem vertices) before enQueue returns.
 * - An in-memory DelayQueue dispatches mails, like MemoryMailQueue.
 * - When processing completes, the record is deleted from YouTrackDB; a mail whose completion was not
 *   recorded before a crash is delivered again after the restart.
 * - On startup, the pending records are loaded back into memory.
 */
public class YouTrackDBMailQueueFactory implements MailQueueFactory<YouTrackDBMailQueueFactory.YouTrackDBMailQueue> {
    private static final Logger LOGGER = LoggerFactory.getLogger(YouTrackDBMailQueueFactory.class);

    private final ConcurrentHashMap<MailQueueName, YouTrackDBMailQueue> mailQueues;
    private final MailQueueItemDecoratorFactory mailQueueItemDecoratorFactory;
    private final YTDBGraphTraversalSource g;
    private final Clock clock;

    @Inject
    public YouTrackDBMailQueueFactory(MailQueueItemDecoratorFactory mailQueueItemDecoratorFactory,
                                      YTDBGraphTraversalSource g,
                                      Clock clock) {
        this.mailQueues = new ConcurrentHashMap<>();
        this.mailQueueItemDecoratorFactory = mailQueueItemDecoratorFactory;
        this.g = g;
        this.clock = clock;
    }

    public YouTrackDBMailQueueFactory(MailQueueItemDecoratorFactory mailQueueItemDecoratorFactory, YTDBGraphTraversalSource g) {
        this(mailQueueItemDecoratorFactory, g, Clock.systemUTC());
    }

    @PreDestroy
    public void clean() {
        mailQueues.values().forEach(YouTrackDBMailQueue::close);
        mailQueues.clear();
    }

    @Override
    public Set<MailQueueName> listCreatedMailQueues() {
        return mailQueues.values()
            .stream()
            .map(YouTrackDBMailQueue::getName)
            .collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public Optional<YouTrackDBMailQueue> getQueue(MailQueueName name, PrefetchCount count) {
        Optional<YouTrackDBMailQueue> queue = Optional.ofNullable(mailQueues.get(name));
        queue.ifPresent(YouTrackDBMailQueue::reference);
        return queue;
    }

    @Override
    public YouTrackDBMailQueue createQueue(MailQueueName name, PrefetchCount prefetchCount) {
        YouTrackDBMailQueue queue = mailQueues.computeIfAbsent(name, mailQueueName ->
            new YouTrackDBMailQueue(mailQueueName, mailQueueItemDecoratorFactory, g, clock));
        queue.reference();
        return queue;
    }

    public static class YouTrackDBMailQueue implements ManageableMailQueue {
        static final String CLASS_NAME = "JamesQueueItem";
        static final String PROP_ENQUEUE_ID = "enqueueId";
        static final String PROP_QUEUE_NAME = "queueName";
        static final String PROP_MAIL_NAME = "mailName";
        static final String PROP_NEXT_DELIVERY = "nextDelivery";
        static final String PROP_SERIALIZED_MAIL = "serializedMail";
        private static final int MAX_MIME_PAYLOAD_SIZE = 100 * 1024 * 1024; // 100 MB max message size safety guard

        private final AtomicInteger references = new AtomicInteger(0);
        private volatile boolean closed = false;
        private final DelayQueue<YouTrackDBMailQueueItem> mailItems;
        private final Set<YouTrackDBMailQueueItem> inProcessingMailItems;
        private final MailQueueName name;
        private final YTDBGraphTraversalSource g;
        private final Flux<MailQueueItem> flux;
        private final Scheduler scheduler;
        private final Scheduler virtualThreadScheduler;
        private final Clock clock;

        public YouTrackDBMailQueue(MailQueueName name,
                                   MailQueueItemDecoratorFactory mailQueueItemDecoratorFactory,
                                   YTDBGraphTraversalSource g,
                                   Clock clock) {
            this.name = name;
            this.g = g;
            this.clock = clock;
            this.mailItems = new DelayQueue<>();
            this.inProcessingMailItems = ConcurrentHashMap.newKeySet();
            this.scheduler = Schedulers.newSingle("ytdb-mail-queue-" + name.asString());
            this.virtualThreadScheduler = Schedulers.fromExecutor(
                java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());

            // Recover persistent items from YouTrackDB on startup
            recoverItemsFromDatabase();

            this.flux = Mono.<YouTrackDBMailQueueItem>create(sink -> {
                    if (closed) {
                        sink.success();
                        return;
                    }
                    try {
                        sink.success(mailItems.poll(10, TimeUnit.MILLISECONDS));
                    } catch (InterruptedException e) {
                        sink.success();
                    }
                })
                .subscribeOn(virtualThreadScheduler)
                .repeat(() -> !closed)
                .subscribeOn(scheduler)
                .flatMap(item ->
                    Mono.fromRunnable(() -> inProcessingMailItems.add(item)).thenReturn(item), 16)
                .map(item -> mailQueueItemDecoratorFactory.decorate(item, name));
        }

        private void recoverItemsFromDatabase() {
            try {
                List<YouTrackDBMailQueueItem> recovered = g.computeInTx(tx -> {
                    List<YouTrackDBMailQueueItem> items = new ArrayList<>();
                    var vertices = tx.V().hasLabel(CLASS_NAME)
                        .has(PROP_QUEUE_NAME, name.asString())
                        .toList();
                    for (var v : vertices) {
                        try {
                            String enqueueId = v.property(PROP_ENQUEUE_ID).isPresent()
                                ? v.value(PROP_ENQUEUE_ID)
                                : UUID.randomUUID().toString();
                            byte[] data = v.value(PROP_SERIALIZED_MAIL);
                            Long nextDeliveryMillis = v.property(PROP_NEXT_DELIVERY).isPresent() ? v.<Long>value(PROP_NEXT_DELIVERY) : 0L;
                            if (data != null && data.length > 0) {
                                Mail mail = deserializeMail(data);
                                ZonedDateTime delivery = Instant.ofEpochMilli(nextDeliveryMillis != null ? nextDeliveryMillis : 0L).atZone(ZoneId.of("UTC"));
                                items.add(new YouTrackDBMailQueueItem(enqueueId, mail, this, clock, delivery));
                            }
                        } catch (Exception e) {
                            LOGGER.error("Failed to recover mail item for queue {}", name.asString(), e);
                        }
                    }
                    return items;
                });
                for (YouTrackDBMailQueueItem item : recovered) {
                    mailItems.put(item);
                }
                if (!recovered.isEmpty()) {
                    LOGGER.info("Recovered {} pending mail queue items for queue {} from YouTrackDB", recovered.size(), name.asString());
                }
            } catch (Exception e) {
                LOGGER.error("Fatal: failed to query persistent queue items from YouTrackDB for queue {}", name.asString(), e);
                throw new IllegalStateException("Failed to recover persistent mail queue items for queue " + name.asString(), e);
            }
        }

        public void reference() {
            references.incrementAndGet();
        }

        @Override
        public void close() {
            if (references.decrementAndGet() <= 0) {
                this.closed = true;
                this.scheduler.dispose();
                this.virtualThreadScheduler.dispose();
                mailItems.forEach(LifecycleUtil::dispose);
                inProcessingMailItems.forEach(LifecycleUtil::dispose);
                mailItems.clear();
                inProcessingMailItems.clear();
            }
        }

        @Override
        public MailQueueName getName() {
            return name;
        }

        @Override
        public void enQueue(Mail mail, Duration delay) throws MailQueueException {
            if (closed) {
                throw new MailQueueException("Mail queue " + name.asString() + " is already closed");
            }
            ZonedDateTime nextDelivery = calculateNextDelivery(delay);
            try {
                // The MIME message is written once; the in-memory copy and the persisted bytes come from it.
                byte[] mime = mail.getMessage() == null ? null : toBytes(mail.getMessage());
                if (mime != null && mime.length > MAX_MIME_PAYLOAD_SIZE) {
                    throw new MailQueueException("MIME message exceeds maximum allowed payload size of "
                        + MAX_MIME_PAYLOAD_SIZE + " bytes (actual: " + mime.length + ")");
                }
                Mail cloned = cloneMail(mail, mime);
                byte[] serialized = serializeMail(cloned, mime);
                String enqueueId = UUID.randomUUID().toString();

                persist(enqueueId, cloned.getName(), nextDelivery, serialized);

                mailItems.put(new YouTrackDBMailQueueItem(enqueueId, cloned, this, clock, nextDelivery));
            } catch (MailQueueException e) {
                throw e;
            } catch (Exception e) {
                throw new MailQueueException("Error while enqueuing mail " + mail.getName() + " to YouTrackDB queue", e);
            }
        }

        private void persist(String enqueueId, String mailName, ZonedDateTime nextDelivery, byte[] serialized) {
            YouTrackDBTransactions.executeStrictTx(g, tx -> insertItem(tx, enqueueId, mailName, nextDelivery, serialized));
        }

        private void insertItem(YTDBGraphTraversalSource tx, String enqueueId, String mailName, ZonedDateTime nextDelivery, byte[] serialized) {
            tx.addV(CLASS_NAME)
                .property(PROP_ENQUEUE_ID, enqueueId)
                .property(PROP_QUEUE_NAME, name.asString())
                .property(PROP_MAIL_NAME, mailName)
                .property(PROP_NEXT_DELIVERY, nextDelivery.toInstant().toEpochMilli())
                .property(PROP_SERIALIZED_MAIL, serialized)
                .iterate();
        }

        @Override
        public Publisher<Void> enqueueReactive(Mail mail) {
            return Mono.fromRunnable(Throwing.runnable(() -> enQueue(mail)).sneakyThrow())
                .subscribeOn(virtualThreadScheduler)
                .then();
        }

        @Override
        public Publisher<Void> enqueueReactive(Mail mail, Duration delay) {
            return Mono.fromRunnable(Throwing.runnable(() -> enQueue(mail, delay)).sneakyThrow())
                .subscribeOn(virtualThreadScheduler)
                .then();
        }

        private ZonedDateTime calculateNextDelivery(Duration delay) {
            if (!delay.isNegative()) {
                try {
                    return ZonedDateTime.now(clock).plus(delay);
                } catch (Exception e) {
                    return Instant.ofEpochMilli(Long.MAX_VALUE).atZone(ZoneId.of("UTC"));
                }
            }
            return ZonedDateTime.now(clock);
        }

        @Override
        public void enQueue(Mail mail) throws MailQueueException {
            enQueue(mail, Duration.ZERO);
        }

        private static byte[] toBytes(MimeMessage message) throws IOException, MessagingException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            message.writeTo(out);
            return out.toByteArray();
        }

        private static MimeMessage parseMime(byte[] mime) throws MessagingException {
            return new MimeMessage(Session.getInstance(new Properties()), new ByteArrayInputStream(mime));
        }

        private Mail cloneMail(Mail mail, byte[] mime) throws MessagingException {
            MailImpl mailImpl = MailImpl.duplicate(mail);
            mailImpl.setName(mail.getName());
            mailImpl.setState(mail.getState());
            mailImpl.addAllSpecificHeaderForRecipient(mail.getPerRecipientSpecificHeaders());
            if (mime != null) {
                mailImpl.setMessage(parseMime(mime));
            }
            return mailImpl;
        }

        /** Same layout as before: the Mail object, a presence flag, then the length-prefixed MIME bytes. */
        private byte[] serializeMail(Mail mail, byte[] mime) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(mime == null ? 1024 : mime.length + 1024);
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(mail);
                oos.writeBoolean(mime != null);
                if (mime != null) {
                    oos.writeInt(mime.length);
                    oos.write(mime);
                }
            }
            return baos.toByteArray();
        }

        private static Mail deserializeMail(byte[] bytes) throws Exception {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                ois.setObjectInputFilter(java.io.ObjectInputFilter.Config.createFilter(
                    "java.base/*;java.lang.*;java.util.*;java.time.*;com.google.common.**;org.apache.james.**;org.apache.mailet.**;!*"
                ));
                Mail mail = (Mail) ois.readObject();
                boolean hasMessage = ois.readBoolean();
                if (hasMessage) {
                    int len = ois.readInt();
                    if (len < 0 || len > MAX_MIME_PAYLOAD_SIZE) {
                        throw new IOException("Corrupted or excessive serialized MIME message length: " + len);
                    }
                    byte[] msgBytes = new byte[len];
                    ois.readFully(msgBytes);
                    mail.setMessage(parseMime(msgBytes));
                }
                return mail;
            }
        }

        @Override
        public Flux<MailQueueItem> deQueue() {
            return flux;
        }

        @Override
        public long getSize() {
            return mailItems.size() + inProcessingMailItems.size();
        }

        @Override
        public long flush() throws MailQueueException {
            List<YouTrackDBMailQueueItem> snapshot = new ArrayList<>(mailItems);
            if (snapshot.isEmpty()) {
                return 0;
            }
            long now = clock.millis();
            try {
                // Database first: if it fails, the in-memory queue is left untouched.
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    for (YouTrackDBMailQueueItem item : snapshot) {
                        var traversal = tx.V().hasLabel(CLASS_NAME)
                            .has(PROP_ENQUEUE_ID, item.getEnqueueId());
                        if (traversal.hasNext()) {
                            traversal.next().property(PROP_NEXT_DELIVERY, now);
                        }
                    }
                });
            } catch (RuntimeException e) {
                throw new MailQueueException("Error while flushing queue " + name.asString(), e);
            }
            ZonedDateTime delivery = Instant.ofEpochMilli(now).atZone(ZoneId.of("UTC"));
            long count = 0;
            for (YouTrackDBMailQueueItem item : snapshot) {
                // An item taken by a consumer meanwhile is no longer in the queue and is skipped.
                if (mailItems.remove(item)) {
                    mailItems.put(new YouTrackDBMailQueueItem(item.getEnqueueId(), item.getMail(), this, clock, delivery));
                    count++;
                }
            }
            return count;
        }

        @Override
        public long clear() {
            // Delete from YouTrackDB first; if DB deletion fails, memory is not corrupted
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesQueueItem WHERE queueName = :queue", "queue", name.asString()));
            int size = mailItems.size();
            mailItems.clear();
            return size;
        }

        @Override
        public long remove(Type type, String value) {
            ImmutableList<YouTrackDBMailQueueItem> toBeRemoved = mailItems.stream()
                .filter(item -> shouldRemove(item, type, value))
                .collect(ImmutableList.toImmutableList());
            if (!toBeRemoved.isEmpty()) {
                if (!closed) {
                    try {
                        YouTrackDBTransactions.executeStrictTx(g, tx -> {
                            for (YouTrackDBMailQueueItem item : toBeRemoved) {
                                var traversal = tx.V().hasLabel(CLASS_NAME)
                                    .has(PROP_ENQUEUE_ID, item.getEnqueueId());
                                if (traversal.hasNext()) {
                                    traversal.next().remove();
                                }
                            }
                        });
                    } catch (Exception e) {
                        LOGGER.warn("Failed batch removal of mail items from queue {}", name.asString(), e);
                    }
                }
                toBeRemoved.forEach(mailItems::remove);
            }
            return toBeRemoved.size();
        }

        private void deleteFromDatabase(String enqueueId) {
            if (closed) {
                return;
            }
            try {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    var traversal = tx.V().hasLabel(CLASS_NAME)
                        .has(PROP_ENQUEUE_ID, enqueueId);
                    if (traversal.hasNext()) {
                        traversal.next().remove();
                    }
                });
            } catch (Exception e) {
                if (!closed) {
                    LOGGER.warn("Failed to delete queue item {} from YouTrackDB queue table", enqueueId, e);
                }
            }
        }

        public boolean shouldRemove(MailQueueItem item, Type type, String value) {
            return switch (type) {
                case Name -> item.getMail().getName().equals(value);
                case Recipient -> item.getMail().getRecipients().stream()
                    .map(MailAddress::asString)
                    .anyMatch(value::equals);
                case Sender -> item.getMail().getMaybeSender()
                    .asString()
                    .equals(value);
                default -> throw new NotImplementedException("Unknown type " + type);
            };
        }

        private void markProcessingAsFinished(YouTrackDBMailQueueItem item, MailQueue.MailQueueItem.CompletionStatus status) {
            inProcessingMailItems.remove(item);
            if (status == MailQueue.MailQueueItem.CompletionStatus.SUCCESS) {
                deleteFromDatabase(item.getEnqueueId());
            } else if (status == MailQueue.MailQueueItem.CompletionStatus.RETRY) {
                try {
                    enQueue(item.getMail());
                } catch (Exception e) {
                    LOGGER.error("Failed to retry mail item {}", item.getMail().getName(), e);
                }
                // Once new retry record is persisted, old queue item record is completed and deleted
                deleteFromDatabase(item.getEnqueueId());
            } else {
                // Any other termination status (e.g. discard/error) -> purge from database so it doesn't resurrect on restart
                deleteFromDatabase(item.getEnqueueId());
            }
        }

        @Override
        public MailQueueIterator browse() {
            Iterator<DefaultMailQueueItemView> underlying = ImmutableList.copyOf(mailItems)
                .stream()
                .map(item -> new DefaultMailQueueItemView(item.getMail(), Optional.of(item.delivery)))
                .iterator();

            return new MailQueueIterator() {
                @Override
                public void close() {
                }

                @Override
                public boolean hasNext() {
                    return underlying.hasNext();
                }

                @Override
                public MailQueueItemView next() {
                    return underlying.next();
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            YouTrackDBMailQueue that = (YouTrackDBMailQueue) o;
            return Objects.equals(this.name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }
    }

    public static class YouTrackDBMailQueueItem implements MailQueue.MailQueueItem, Delayed {
        private final String enqueueId;
        private final Mail mail;
        private final YouTrackDBMailQueue queue;
        private final Clock clock;
        private final ZonedDateTime delivery;
        private final long deliveryMillis;

        public YouTrackDBMailQueueItem(String enqueueId, Mail mail, YouTrackDBMailQueue queue, Clock clock, ZonedDateTime delivery) {
            this.enqueueId = Objects.requireNonNull(enqueueId, "enqueueId must not be null");
            this.mail = mail;
            this.queue = queue;
            this.clock = clock;
            this.delivery = delivery;
            this.deliveryMillis = toEpochMillisSaturated(delivery);
        }

        public String getEnqueueId() {
            return enqueueId;
        }

        private static long toEpochMillisSaturated(ZonedDateTime time) {
            try {
                return time.toInstant().toEpochMilli();
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
        }

        @Override
        public Mail getMail() {
            return mail;
        }

        @Override
        public void done(CompletionStatus success) throws MailQueue.MailQueueException {
            queue.markProcessingAsFinished(this, success);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(deliveryMillis - clock.millis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            if (other instanceof YouTrackDBMailQueueItem item) {
                return Long.compare(deliveryMillis, item.deliveryMillis);
            }
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), other.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
