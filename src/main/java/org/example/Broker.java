package org.example;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Broker {
    private final Map<String, Topic> topics = new ConcurrentHashMap<>();

    public Topic createTopic(String name, Duration retention) {
        Topic t = new Topic(name, retention);
        topics.put(name, t);
        return t;
    }

    public void deleteTopic(String name) {
        Topic t = topics.remove(name);
        if (t != null) {
            t.close(); // cleanup resources
            System.out.printf("Topic %s deleted%n", name);
        }
    }

    public Publisher createPublisher(String topicName) {
        Topic t = topics.get(topicName);
        if (t == null) throw new IllegalArgumentException("No such topic: " + topicName);
        return new Publisher(t);
    }

    public Consumer createConsumer(String topicName, String id) {
        Topic t = topics.get(topicName);
        if (t == null) throw new IllegalArgumentException("No such topic: " + topicName);
        return new Consumer(t, id);
    }

    public String getConsumerStats(String topicName, String consumerId) {
        Topic t = topics.get(topicName);
        if (t == null) return "Topic not found";
        Consumer c = t.getConsumer(consumerId);
        if (c == null) return "Consumer not found";

        long latest = t.getLatestOffset();
        long lag = Math.max(0, latest - c.getNextOffset() + 1);
        return String.format("Consumer=%s nextOffset=%d latestOffset=%d lag=%d",
                c.getId(), c.getNextOffset(), latest, lag);
    }
}
