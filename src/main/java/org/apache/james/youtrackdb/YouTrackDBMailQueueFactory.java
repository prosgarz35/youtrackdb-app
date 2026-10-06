package org.apache.james.youtrackdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.mail.MessagingException;
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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Temporals;

import com.github.fge.lambdas.Throwing;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * ACID Persistent MailQueue backed by YouTrackDB:
 * - Every enqueued mail is stored in YouTrackDB (under JamesQueueItem class) ensuring zero loss across crashes/restarts.
 * - In-memory DelayQueue provides ultra-low latency dequeue dispatch matching MemoryMailQueue speed.
 * - On deQueue completion, the record is atomically deleted from YouTrackDB.
 * - On server startup, any pending in-flight messages in YouTrackDB are loaded back into memory.
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
        static final String PROP_QUEUE_NAME = "queueName";
        static final String PROP_MAIL_NAME = "mailName";
        static final String PROP_NEXT_DELIVERY = "nextDelivery";
        static final String PROP_SERIALIZED_MAIL = "serializedMail";

        private final AtomicInteger references = new AtomicInteger(0);
        private volatile boolean closed = false;
        private final DelayQueue<YouTrackDBMailQueueItem> mailItems;
        private final LinkedBlockingDeque<YouTrackDBMailQueueItem> inProcessingMailItems;
        private final MailQueueName name;
        private final YTDBGraphTraversalSource g;
        private final Flux<MailQueueItem> flux;
        private final Scheduler scheduler;
        private final Clock clock;

        public YouTrackDBMailQueue(MailQueueName name,
                                   MailQueueItemDecoratorFactory mailQueueItemDecoratorFactory,
                                   YTDBGraphTraversalSource g,
                                   Clock clock) {
            this.name = name;
            this.g = g;
            this.clock = clock;
            this.mailItems = new DelayQueue<>();
            this.inProcessingMailItems = new LinkedBlockingDeque<>();
            this.scheduler = Schedulers.newSingle("ytdb-mail-queue-" + name.asString());

            // Recover persistent items from YouTrackDB on startup
            recoverItemsFromDatabase();

            this.flux = Mono.<YouTrackDBMailQueueItem>create(sink -> {
                    try {
                        sink.success(mailItems.poll(10, TimeUnit.MILLISECONDS));
                    } catch (InterruptedException e) {
                        sink.success();
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .repeat()
                .subscribeOn(scheduler)
                .flatMap(item ->
                    Mono.fromRunnable(() -> inProcessingMailItems.add(item)).thenReturn(item), 16)
                .map(item -> mailQueueItemDecoratorFactory.decorate(item, name));
        }

        private void recoverItemsFromDatabase() {
            try {
                List<YouTrackDBMailQueueItem> recovered = g.computeInTx(tx -> {
                    List<YouTrackDBMailQueueItem> items = new ArrayList<>();
                    var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_QUEUE_NAME, name.asString());
                    while (traversal.hasNext()) {
                        Vertex v = traversal.next();
                        try {
                            byte[] data = v.value(PROP_SERIALIZED_MAIL);
                            Long nextDeliveryMillis = v.property(PROP_NEXT_DELIVERY).isPresent() ? v.value(PROP_NEXT_DELIVERY) : 0L;
                            Mail mail = deserializeMail(data);
                            ZonedDateTime delivery = Instant.ofEpochMilli(nextDeliveryMillis).atZone(ZoneId.of("UTC"));
                            items.add(new YouTrackDBMailQueueItem(mail, this, clock, delivery));
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
                LOGGER.warn("Failed to query persistent queue items from YouTrackDB for queue {}", name.asString(), e);
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
            ZonedDateTime nextDelivery = calculateNextDelivery(delay);
            try {
                Mail cloned = cloneMail(mail);
                byte[] serialized = serializeMail(cloned);

                // Persist into YouTrackDB transactionally
                g.executeInTx(tx -> {
                    tx.addV(CLASS_NAME)
                        .property(PROP_QUEUE_NAME, name.asString())
                        .property(PROP_MAIL_NAME, cloned.getName())
                        .property(PROP_NEXT_DELIVERY, nextDelivery.toInstant().toEpochMilli())
                        .property(PROP_SERIALIZED_MAIL, serialized)
                        .iterate();
                });

                mailItems.put(new YouTrackDBMailQueueItem(cloned, this, clock, nextDelivery));
            } catch (Exception e) {
                throw new MailQueueException("Error while enqueuing mail " + mail.getName() + " to YouTrackDB queue", e);
            }
        }

        @Override
        public Publisher<Void> enqueueReactive(Mail mail) {
            return Mono.fromRunnable(Throwing.runnable(() -> enQueue(mail)).sneakyThrow())
                .subscribeOn(Schedulers.boundedElastic())
                .then();
        }

        @Override
        public Publisher<Void> enqueueReactive(Mail mail, Duration delay) {
            return Mono.fromRunnable(Throwing.runnable(() -> enQueue(mail, delay)).sneakyThrow())
                .subscribeOn(Schedulers.boundedElastic())
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

        private Mail cloneMail(Mail mail) throws MessagingException {
            MailImpl mailImpl = MailImpl.duplicate(mail);
            mailImpl.setName(mail.getName());
            mailImpl.setState(mail.getState());
            mailImpl.addAllSpecificHeaderForRecipient(mail.getPerRecipientSpecificHeaders());
            Optional.ofNullable(mail.getMessage())
                .ifPresent(Throwing.consumer(message -> mailImpl.setMessage(new MimeMessage(message))));
            return mailImpl;
        }

        private byte[] serializeMail(Mail mail) throws Exception {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(mail);
            }
            return baos.toByteArray();
        }

        private static Mail deserializeMail(byte[] bytes) throws Exception {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
                return (Mail) ois.readObject();
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
            int count = 0;
            for (YouTrackDBMailQueueItem item : mailItems) {
                if (mailItems.remove(item)) {
                    enQueue(item.getMail());
                    count += 1;
                }
            }
            return count;
        }

        @Override
        public long clear() {
            int size = mailItems.size();
            mailItems.clear();
            // Remove from YouTrackDB via set-based YQL deletion
            g.executeInTx(tx -> {
                tx.command("DELETE VERTEX JamesQueueItem WHERE queueName = ?", name.asString());
            });
            return size;
        }

        @Override
        public long remove(Type type, String value) {
            ImmutableList<YouTrackDBMailQueueItem> toBeRemoved = mailItems.stream()
                .filter(item -> shouldRemove(item, type, value))
                .collect(ImmutableList.toImmutableList());
            toBeRemoved.forEach(item -> {
                mailItems.remove(item);
                deleteFromDatabase(item.getMail().getName());
            });
            return toBeRemoved.size();
        }

        private void deleteFromDatabase(String mailName) {
            if (closed) {
                return;
            }
            try {
                g.executeInTx(tx -> {
                    tx.command("DELETE VERTEX JamesQueueItem WHERE queueName = ? AND mailName = ?", name.asString(), mailName);
                });
            } catch (Exception e) {
                if (!closed) {
                    LOGGER.warn("Failed to delete mail {} from YouTrackDB queue table", mailName, e);
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
                Schedulers.boundedElastic().schedule(() -> deleteFromDatabase(item.getMail().getName()));
            } else if (status == MailQueue.MailQueueItem.CompletionStatus.RETRY) {
                try {
                    enQueue(item.getMail());
                } catch (Exception e) {
                    LOGGER.error("Failed to retry mail item {}", item.getMail().getName(), e);
                }
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
        private final Mail mail;
        private final YouTrackDBMailQueue queue;
        private final Clock clock;
        private final ZonedDateTime delivery;

        public YouTrackDBMailQueueItem(Mail mail, YouTrackDBMailQueue queue, Clock clock, ZonedDateTime delivery) {
            this.mail = mail;
            this.queue = queue;
            this.clock = clock;
            this.delivery = delivery;
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
            try {
                return ZonedDateTime.now(clock).until(delivery, Temporals.chronoUnit(unit));
            } catch (ArithmeticException e) {
                return Long.MAX_VALUE;
            }
        }

        @Override
        public int compareTo(Delayed o) {
            return Math.toIntExact(getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
