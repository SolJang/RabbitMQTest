package com.example.recv.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class FanoutRecv {
    private static final String FANOUT_EXCHANGE_NAME = "fanoutTest";


    public static void main(String[] args) throws Exception {
        System.out.println("[FanoutRecv] Start");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        Connection con = factory.newConnection();
        Channel channel = con.createChannel();

        channel.exchangeDeclare(FANOUT_EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();

        // fanout은 routingKey을 무시
        channel.queueBind(queueName, FANOUT_EXCHANGE_NAME, "");


        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[FanoutRecv] routingKey = " + delivery.getEnvelope().getRoutingKey() +" : message = " + message);
        });

        channel.basicConsume(queueName, true, dc, consumerTag -> {});

    }
}
