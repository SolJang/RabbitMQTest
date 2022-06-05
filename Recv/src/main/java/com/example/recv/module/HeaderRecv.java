package com.example.recv.module;

import com.rabbitmq.client.*;
import org.springframework.amqp.core.ExchangeTypes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HeaderRecv {
    private static final String HEADER_EXCHANGE_NAME = "headerTest";
    private static final String HEADER_QUEUE_NAME1 = "headerQueue1";
    private static final String HEADER_QUEUE_NAME2 = "headerQueue2";
    private static final String HEADER_QUEUE_NAME3 = "headerQueue3";

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
        Map<String, Object> headers = null;
        headers = new HashMap<String, Object>();
        headers.put("x-match", "any");
        headers.put("first", "A");
        headers.put("fourth", "D");
        channel.queueDeclare(HEADER_QUEUE_NAME1, true, false, false, null);
        channel.queueBind(HEADER_QUEUE_NAME1, HEADER_EXCHANGE_NAME, "", headers);

        headers = new HashMap<String, Object>();
        headers.put("x-match", "any");
        headers.put("fourth", "D");
        headers.put("third", "C");
        channel.queueDeclare(HEADER_QUEUE_NAME2, true, false, false, null);
        channel.queueBind(HEADER_QUEUE_NAME2, HEADER_EXCHANGE_NAME, "", headers);

        headers = new HashMap<String, Object>();
        headers.put("x-match", "all");
        headers.put("first", "A");
        headers.put("third", "C");
        channel.queueDeclare(HEADER_QUEUE_NAME3, true, false, false, null);
        channel.queueBind(HEADER_QUEUE_NAME3, HEADER_EXCHANGE_NAME, "", headers);
        Consumer consumer1 = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" Message Received Queue 1 '" + message + "'");
            }
        };
        channel.basicConsume(HEADER_QUEUE_NAME1, true, consumer1);

        Consumer consumer2 = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" Message Received Queue 2 '" + message + "'");
            }
        };
        channel.basicConsume(HEADER_QUEUE_NAME2, true, consumer2);

       Consumer consumer3 = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println(" Message Received Queue 3 '" + message + "'");
            }
        };
        channel.basicConsume(HEADER_QUEUE_NAME3, true, consumer3);


    }

}
