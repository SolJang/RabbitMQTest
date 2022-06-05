package com.example.sender.module;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.amqp.core.ExchangeTypes;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HeaderSender {
    private static final String HEADER_EXCHANGE_NAME = "headerTest";


    public static void main(String[] args) throws Exception {
        System.out.println("[HeaderSender] Start");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {
            // 1. fanout exchange 생성
            channel.exchangeDeclare(HEADER_EXCHANGE_NAME, ExchangeTypes.HEADERS);
            Map<String, Object> headers = null;
            headers = new HashMap<String, Object>();
            headers.put("first", "A");
            headers.put("fourth", "D");
            headers.put("third", "C");

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().headers(headers).build();
            String message = "first header message send";
            // 2. Producer 작업
            channel.basicPublish(HEADER_EXCHANGE_NAME,
                    "",
                    props,
                    message.getBytes(StandardCharsets.UTF_8));




            headers = new HashMap<String, Object>();
            headers.put("third", "C");

            props = new AMQP.BasicProperties.Builder().headers(headers).build();
            message = "second header message send";
            // 2. Producer 작업
//            channel.basicPublish(HEADER_EXCHANGE_NAME,
//                    "",
//                    props,
//                    message.getBytes(StandardCharsets.UTF_8));





            headers = new HashMap<String, Object>();
            headers.put("first", "A");
            headers.put("third", "C");

            props = new AMQP.BasicProperties.Builder().headers(headers).build();
            message = "third header message send";
            // 2. Producer 작업
//            channel.basicPublish(HEADER_EXCHANGE_NAME,
//                    "",
//                    props,
//                    message.getBytes(StandardCharsets.UTF_8));

        }

    }
}
