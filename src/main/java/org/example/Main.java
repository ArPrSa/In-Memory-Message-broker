package org.example;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        Topic orders = broker.createTopic("orders", Duration.ofSeconds(30));

        Publisher p1 = broker.createPublisher("orders");
        Publisher p2 = broker.createPublisher("orders");

        Consumer c1 = broker.createConsumer("orders", "C1");
        c1.consume();

        ExecutorService pool = Executors.newFixedThreadPool(4);
        for (int i = 0; i < 5; i++) {
            int id = i;
            pool.submit(() -> p1.publish("p1-msg-" + id));
            pool.submit(() -> p2.publish("p2-msg-" + id));
        }

        Thread.sleep(2000);

        System.out.println(broker.getConsumerStats("orders", "C1"));

        System.out.println("Resetting offset to 2...");
        c1.resetOffset(2);

        Thread.sleep(2000);
        System.out.println(broker.getConsumerStats("orders", "C1"));

        // Stop consumer and delete topic
        c1.stop();
        broker.deleteTopic("orders");

        pool.shutdown();
    }
}