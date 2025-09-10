package org.example.drivers;

import org.example.Broker;
import org.example.Consumer;
import org.example.Publisher;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Driver2_TwoPubOneConsumer {
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        broker.createTopic("orders", Duration.ofSeconds(30));

        Publisher p1 = broker.createPublisher("orders");
        Publisher p2 = broker.createPublisher("orders");

        Consumer c1 = broker.createConsumer("orders", "C1");
        c1.consume();

        ExecutorService pool = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 5; i++) {
            int id = i;
            pool.submit(() -> p1.publish("p1-msg-" + id));
            pool.submit(() -> p2.publish("p2-msg-" + id));
        }

        Thread.sleep(2000);
        System.out.println(broker.getStats("orders", "C1"));

        c1.stop();
        broker.deleteTopic("orders");
        pool.shutdown();
    }
}
