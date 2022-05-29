package com.example.sender.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

public class TopicSender {
    private static final String TOPIC_EXCHANGE_NAME = "topicTest";


    public static void main(String[] args) throws Exception {
        System.out.println("[TopicSender] Start");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {
            // 1. fanout exchange 생성
            channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, "topic");

            String[] routeTest = new String[]{"aaa", "aaa.orange.apple", "hi.orange.black", "lazy", "lazy.test.black", "lazy.orange"};
            for(String route : routeTest){
                String message = "Topic Test Message => ["+ route + "]";
                System.out.println("message = " + message);

                // 2. Producer 작업
                channel.basicPublish(TOPIC_EXCHANGE_NAME,
                        route,
                        null,
                        message.getBytes(StandardCharsets.UTF_8));
            }

        }

    }
}
