package org.example.drivers;

import org.example.Broker;
import org.example.Consumer;
import org.example.Publisher;
import java.time.Duration;

public class Driver3_OnePublisherTwoConsumers {
    public static void main(String[] args) throws InterruptedException {
        Broker broker = new Broker();
        broker.createTopic("orders", Duration.ofSeconds(30));

        Publisher p1 = broker.createPublisher("orders");

        Consumer c1 = broker.createConsumer("orders", "C1");
        Consumer c2 = broker.createConsumer("orders", "C2");
        c1.consume();
        c2.consume();

        p1.publish("msg-1");
        p1.publish("msg-2");
        p1.publish("msg-3");

        Thread.sleep(2000);

        System.out.println(broker.getStats("orders", "C1"));
        System.out.println(broker.getStats("orders", "C2"));

        c1.stop();
        c2.stop();
        broker.deleteTopic("orders");
    }
}
