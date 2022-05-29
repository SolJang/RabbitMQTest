package com.example.recv.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class TopicRecv {
    private static final String TOPIC_EXCHANGE_NAME = "topicTest";


    public static void main(String[] args) throws Exception {
        System.out.println("[TopicRecv] Start");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        Connection con = factory.newConnection();
        Channel channel = con.createChannel();

        queue1(channel);
        queue2(channel);

    }

    private static void queue1(Channel channel) throws Exception {
        channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, "*.orange.*");

        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[TopicRecv => " + queueName +"] routingKey = " + delivery.getEnvelope().getRoutingKey() +" : message = " + message);
        });

        channel.basicConsume(queueName, true, dc, consumerTag -> {});
    }

    private static void queue2(Channel channel) throws Exception {
        channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, "*.*.black");
        channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, "lazy.#");


        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[TopicRecv => " + queueName +"] routingKey = " + delivery.getEnvelope().getRoutingKey() +" : message = " + message);
        });

        channel.basicConsume(queueName, true, dc, consumerTag -> {});
    }
}
