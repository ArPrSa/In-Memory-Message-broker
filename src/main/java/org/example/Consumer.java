package org.example;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Consumer {
    private final Topic topic;
    private final String id;
    private final ExecutorService worker = Executors.newSingleThreadExecutor();
    private volatile long nextOffset;
    private volatile boolean running = true;

    public Consumer(Topic topic, String id) {
        this.topic = topic;
        this.id = id;
        this.nextOffset = topic.getBaseOffset();
        topic.registerConsumer(this);
    }

    public String getId() {
        return id;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void consume() {
        worker.submit(() -> {
            while (running) {
                synchronized (topic) {
                    while (running && topic.getMessagesFromOffset(nextOffset).isEmpty()) {
                        try {
                            topic.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    List<Message> msgs = topic.getMessagesFromOffset(nextOffset);
                    for (Message m : msgs) {
                        try {
                            System.out.printf("Consumer %s got offset=%d payload=%s%n",
                                    id, m.getOffset(), m.getPayload());
                        } catch (Exception e) {
                            System.err.println("Error processing message: " + e.getMessage());
                        }
                        nextOffset = m.getOffset() + 1;
                    }
                }
            }
        });
    }

    public void stop() {
        running = false;
        worker.shutdownNow();
        synchronized (topic) {
            topic.notifyAll();
        }
        System.out.printf("Consumer %s stopped%n", id);
    }

    public void resetOffset(long newOffset) {
        if (newOffset < topic.getBaseOffset()) {
            nextOffset = topic.getBaseOffset();
        } else {
            nextOffset = newOffset;
        }
    }
}

