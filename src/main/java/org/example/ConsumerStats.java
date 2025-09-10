package org.example;


public class ConsumerStats {
    private final String consumerId;
    private final long nextOffset;
    private final long latestOffset;
    private final long lag;

    public ConsumerStats(String consumerId, long nextOffset, long latestOffset, long lag) {
        this.consumerId = consumerId;
        this.nextOffset = nextOffset;
        this.latestOffset = latestOffset;
        this.lag = lag;
    }

    public String getConsumerId() {
        return consumerId;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public long getLatestOffset() {
        return latestOffset;
    }

    public long getLag() {
        return lag;
    }

    @Override
    public String toString() {
        return String.format("Consumer=%s nextOffset=%d latestOffset=%d lag=%d",
                consumerId, nextOffset, latestOffset, lag);
    }
}

