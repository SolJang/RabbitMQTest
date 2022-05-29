package com.example.recv.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class DirectRecv {
    private static final String DIRECT_EXCHANGE_NAME = "directTest";


    public static void main(String[] args) throws Exception {
        System.out.println("[DirectRecv] Start");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        Connection con = factory.newConnection();
        Channel channel = con.createChannel();

        channel.queueDeclare("queue1", true, false, false, null);
        channel.queueDeclare("queue2", true, false, false, null);



        // 1. direct exchange 생성
        channel.exchangeDeclare(DIRECT_EXCHANGE_NAME, "direct");
        channel.queueBind("queue1", DIRECT_EXCHANGE_NAME, "error");
        channel.queueBind("queue1", DIRECT_EXCHANGE_NAME, "info");
        channel.queueBind("queue2", DIRECT_EXCHANGE_NAME, "error");

        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[DirectRecv] routingKey = " + consumerTag + delivery.getEnvelope().getRoutingKey() +" : message = " + message);
        });

        channel.basicConsume("queue1", true, dc, consumerTag -> {});
        channel.basicConsume("queue2", true, dc, consumerTag -> {});


    }
}
