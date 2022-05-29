package com.example.sender.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Date;

public class DirectSender {
    private static final String DIRECT_EXCHANGE_NAME = "directTest";


    public static void main(String[] args) throws Exception {
        System.out.println("[DirectSender] Start");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {
            // 1. direct exchange 생성
            channel.exchangeDeclare(DIRECT_EXCHANGE_NAME, "direct");

            String[] routeArr = new String[]{"error", "black", "pink", "info"};

            for (String route : routeArr ){
                Date date = new Date();
                String message = "direct Test Message => ["+ route + "] " + date.getTime();
                System.out.println("message = " + message);

                // 2. Producer 작업
                channel.basicPublish(DIRECT_EXCHANGE_NAME,
                        route,
                        null,
                        message.getBytes(StandardCharsets.UTF_8));
            }

        }

    }
}
