package com.example.sender.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

public class FanoutSender {
    private static final String FANOUT_EXCHANGE_NAME = "fanoutTest";


    public static void main(String[] args) throws Exception {
        System.out.println("[FanoutSender] Start");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {
            // 1. fanout exchange 생성
            channel.exchangeDeclare(FANOUT_EXCHANGE_NAME, "fanout");

            String Str = "RABBITMQ FANOUT EXCHANGE TEST!!";
            for (char ch : Str.toCharArray() ){
                String message = "Fanout Test Message => ["+ ch + "]";
                System.out.println("message = " + message);

                // 2. Producer 작업
                channel.basicPublish(FANOUT_EXCHANGE_NAME,
                        "",
                        null,
                        message.getBytes(StandardCharsets.UTF_8));
            }

        }

    }
}
