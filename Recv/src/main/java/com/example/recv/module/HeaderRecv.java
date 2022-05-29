package com.example.recv.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.springframework.amqp.core.ExchangeTypes;

import java.nio.charset.StandardCharsets;

public class HeaderRecv {
    private static final String HEADER_EXCHANGE_NAME = "headerTest";


    public static void main(String[] args) throws Exception {
        System.out.println("[HeaderRecv] Start");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        Connection con = factory.newConnection();
        Channel channel = con.createChannel();

        queue1(channel);

    }

    private static void queue1(Channel channel) throws Exception {
        channel.exchangeDeclare(HEADER_EXCHANGE_NAME, ExchangeTypes.HEADERS);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, HEADER_EXCHANGE_NAME, "");

        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[HeaderRecv => " + queueName +"] routingKey = " + delivery.getEnvelope().getRoutingKey() +" : message = " + message);
        });

        channel.basicConsume(queueName, true, dc, consumerTag -> {});
    }

}
