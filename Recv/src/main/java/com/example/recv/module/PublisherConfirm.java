package com.example.recv.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class PublisherConfirm {
    private static Connection connection;
    private static Channel channel;

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        connection = factory.newConnection();
        channel = connection.createChannel();
        // publisher confirm : 메시지 전송도 안 잃어 버리기
        // 메시지를 처리할 때만 ack를 보내는게 아니라 메시지를 정상적으로 보냈는지 확인할 ack 도 받고싶을 떄 사용
        // 대신 publisher confirm과 transaction은 함께 사용할 수 없다.
        // 아래처럼 channel.confirmSelect()를 선언하고 메시지를 보내면 callback을 받을 수 있다.
        channel.confirmSelect();

    }
}
