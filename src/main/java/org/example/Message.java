package org.example;

import java.time.Instant;

public class Message {
    private final long offset;
    private final String payload;
    private final Instant enqueuedAt;

    public Message(long offset, String payload) {
        this.offset = offset;
        this.payload = payload;
        this.enqueuedAt = Instant.now();
    }

    public long getOffset() {
        return offset;
    }

    public String getPayload() {
        return payload;
    }

    public Instant getEnqueuedAt() {
        return enqueuedAt;
    }
}

