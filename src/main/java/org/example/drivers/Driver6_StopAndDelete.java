package org.example.drivers;

import org.example.Broker;
import org.example.Consumer;
import org.example.Publisher;

import java.time.Duration;

public class Driver6_StopAndDelete {
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        broker.createTopic("orders", Duration.ofSeconds(30));

        Publisher p1 = broker.createPublisher("orders");
        Consumer c1 = broker.createConsumer("orders", "C1");
        c1.consume();

        p1.publish("msg-1");
        Thread.sleep(1000);

        System.out.println(">>> Stopping consumer...");
        c1.stop();

        System.out.println(">>> Deleting topic...");
        broker.deleteTopic("orders");

       // If trying now, publish will fail
        try {
            p1.publish("msg-2");
        } catch (Exception e) {
            System.out.println("Expected error: " + e.getMessage());
        }
    }
}
