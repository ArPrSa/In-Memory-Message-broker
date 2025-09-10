package org.example;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Topic {
    private final String name;
    private final Duration retention;
    private final List<Message> messages = new ArrayList<>();
    private long baseOffset = 0;
    private final AtomicLong nextOffset = new AtomicLong(0);

    private final ScheduledExecutorService cleaner =
            Executors.newSingleThreadScheduledExecutor();

    // track consumers for visibility API
    private final ConcurrentMap<String, Consumer> consumers = new ConcurrentHashMap<>();

    public Topic(String name, Duration retention) {
        this.name = name;
        this.retention = retention;
        cleaner.scheduleAtFixedRate(this::cleanup, 5, 5, TimeUnit.SECONDS);
    }

    public String getName() {
        return name;
    }

    public synchronized Message publish(String payload) {
        Message msg = new Message(nextOffset.getAndIncrement(), payload);
        messages.add(msg);
        notifyAll();
        return msg;
    }

    public synchronized List<Message> getMessagesFromOffset(long offset) {
        if (messages.isEmpty()) return Collections.emptyList();
        if (offset < baseOffset) offset = baseOffset;
        int start = (int) (offset - baseOffset);
        if (start >= messages.size()) return Collections.emptyList();
        return new ArrayList<>(messages.subList(start, messages.size()));
    }

    private synchronized void cleanup() {
        Instant cutoff = Instant.now().minus(retention);
        while (!messages.isEmpty() && messages.get(0).getEnqueuedAt().isBefore(cutoff)) {
            messages.remove(0);
            baseOffset++;
        }
    }

    public long getLatestOffset() {
        return nextOffset.get() - 1;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public void registerConsumer(Consumer consumer) {
        consumers.put(consumer.getId(), consumer);
    }

    public Consumer getConsumer(String id) {
        return consumers.get(id);
    }

    public void close() {
        cleaner.shutdownNow();
    }

}

