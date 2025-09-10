package org.example.drivers;

import org.example.Broker;
import org.example.Consumer;
import org.example.Publisher;

import java.time.Duration;

public class Driver5_ResetOffset {
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        broker.createTopic("orders", Duration.ofSeconds(30));

        Publisher p1 = broker.createPublisher("orders");
        Consumer c1 = broker.createConsumer("orders", "C1");
        c1.consume();

        p1.publish("msg-1");
        p1.publish("msg-2");
        p1.publish("msg-3");

        Thread.sleep(1000);
        System.out.println(">>> Stats before reset:");
        System.out.println(broker.getStats("orders", "C1"));

        System.out.println(">>> Resetting offset to 1...");
        c1.resetOffset(1);

        Thread.sleep(2000);
        System.out.println(">>> Stats after reset:");
        System.out.println(broker.getStats("orders", "C1"));

        c1.stop();
        broker.deleteTopic("orders");
    }
}
