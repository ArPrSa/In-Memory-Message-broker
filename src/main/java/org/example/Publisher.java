package org.example;

public class Publisher {
    private final Topic topic;

    public Publisher(Topic topic) {
        this.topic = topic;
    }

    public Message publish(String payload) {
        return topic.publish(payload);
    }
}

