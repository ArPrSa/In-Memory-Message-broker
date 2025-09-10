package org.example.drivers;

import org.example.Broker;
import org.example.Consumer;
import org.example.Publisher;

import java.time.Duration;

public class Driver4_RetentionTest {
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        broker.createTopic("orders", Duration.ofSeconds(2)); // 2s retention

        Publisher p1 = broker.createPublisher("orders");
        Consumer c1 = broker.createConsumer("orders", "C1");

        p1.publish("msg-1");
        Thread.sleep(7000); // wait beyond retention
        c1.consume();
        p1.publish("msg-2");

        Thread.sleep(2000);

        System.out.println(">>> Stats after retention:");
        System.out.println(broker.getConsumerStats("orders", "C1"));

        c1.stop();
        broker.deleteTopic("orders");
    }

}
