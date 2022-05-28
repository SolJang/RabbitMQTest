package com.example.recv.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class RecvModule {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        // Connection : Application과 RabbitMQ broker 사이의 TCP 연결
        Connection connection = factory.newConnection();
        // Channel : connection 내부에 정의된 가상 연결. Queue에서 데이터를 손볼 때 생시는 일종의 통로같은 개념
        Channel channel = connection.createChannel();

        /** queueDeclare : Queue를 선언
         * channel.queueDeclare(String queue, boolean durable, boolean exclusive,
         *          boolean autoDelete, Map<String,Object> arguments)
         * queue : 큐 이름
         * durable : 서버 재시작에도 살아남을 튼튼한 큐로 선언할 것 인지 여부
         * exclusive : 현재의 연결에 한정되는 베타적인 큐로 선언할 것인지 여부
         * autoDelete : 사용되지 않을 때 서버에 의해 자동 삭제되는 큐로 선언할 것인지 여부
         * arguments : 큐를 구성하는 다른 속성
         */
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("RECV message = " + message);
            try{
//                doWork(message);
            } catch(Exception e){
                System.out.println(e.getMessage());
            } finally {
                // BassicAck 메소드를 사용하여 메세지 처리가 완료되면 Queue에서 지우도록 Ack 전송
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        });
        boolean autoAck = false;

        // Queue에서 Consumer로 메시지 전송이 완료되는 순간 Consumer가 Queue에게 승인을 넘기고 Queue는 승인을 받아야만
        // 다음 데이터를 Consumer에게 넘겨준다.
        // 그래서 승인이 중요하다.. 승인을 안해주면 Consumer가 멈추게 된다.
        // 문제는 Consumer가 승인을 보내지않고 커넥션을 끊은 후 다시 접속하면
        // 데이터는 복구되기 위해 큐에 다시 적재되는데 이경우 이미 처리한 데이터를 또 처리하게 된다. 그래서 반드시 승인해줘야한다.
        channel.basicConsume(QUEUE_NAME, autoAck, dc, consumerTag -> {});
    }

    private static void doWork(String task) throws InterruptedException {
        for(char ch : task.toCharArray()){
            if(ch == 'H') Thread.sleep(1000);
        }
    }
}
