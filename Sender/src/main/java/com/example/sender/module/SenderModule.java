package com.example.sender.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.nio.charset.StandardCharsets;

public class SenderModule {

    private final static String QUEUE_NAME = "hello";
    private final static String TASK_QUEUE_NAME = "task_queue";
    private static final String EXCHANGE_NAME = "logs";
    private static final String DIRECT_EXCHANGE_NAME = "logsDirect";
    private static final String TOPIC_EXCHANGE_NAME = "logsTopic";

    public static void main(String[] args) throws Exception {
        System.out.println("주기적 호출");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

//        helloQueue(factory);
//        taskQueue(factory);
//        fanoutQueue(factory);
        directQueue(factory);
        topicQueue(factory);
    }

    public static void helloQueue(ConnectionFactory factory) throws Exception{
        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {
            for(int i=0; i<=1000; i++){

                /** queueDeclare : Queue를 선언하는 메서드
                 * channel.queueDeclare(String queue, boolean durable, boolean exclusive,
                 *          boolean autoDelete, Map<String,Object> arguments)
                 * queue : 큐 이름
                 * durable : 서버 재시작에도 살아남을 튼튼한 큐로 선언할 것 인지 여부 (내구성)
                 *          => 해당 큐에 대해 내구성을 선택하면 수정할 수 없음
                 *          => 이건 보내는곳(publish)과 받는곳(consumer)가 동일해야함
                 * exclusive : 현재의 연결에 한정되는 베타적인 큐로 선언할 것인지 여부
                 * autoDelete : 사용되지 않을 때 서버에 의해 자동 삭제되는 큐로 선언할 것인지 여부
                 * arguments : 큐를 구성하는 다른 속성
                 */
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);


                // 파라미터 없이 queueDeclare 메서드를 사용하면 서버에의해 명명된 배타적이고 자동 삭제 되며 튼튼하지 않은 큐로 선언
                // 대신 사용 => exchangeDeclare
                /** exchangeDeclare : 다른 속성없이 자동 삭제 되지 않고 튼튼하지 않은 거래소를 선언하는 메서드
                 * exchangeDeclare(String exchange, BuiltinExchangeType type)
                 * exchange : 거래소의 이름
                 * type : 거래소 유형 (fanout, direct, topic...)
                 */
                String message = "Hello World => " + i;

                /** basicPublish : 메시지를 발행(Public)하는 메서드
                 * channel.basicPublish(String exchange, String routingKey, boolean mandatory,
                 *          AMQP.BasicProperties props, byte[] body)
                 * exchange : 메시지를 발행하는 거래소
                 *         => "" 빈값으로 보내게 되면 routingKey값으로 라우팅된다.
                 *         => 즉 exchange가 있거나 routingKey값이 있어야한다.
                 * routingKey : 라우팅 키
                 * mandatory : 필수 여부
                 * props : 라우팅 헤더 등 메시지의 다른 속성들
                 *      => MessageProperties.PERSISTENT_TEXT_PLAIN 지속성
                 * body : 메시지 본문
                 */
                channel.basicPublish("",
                        QUEUE_NAME,
                        null,
                        message.getBytes());
                System.out.println("SEND message = " + message);

                Thread.sleep(10);
            }
        }
    }

    public static void taskQueue(ConnectionFactory factory) throws Exception{
        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {
            // durable : 서버 재시작에도 살아남을 튼튼한 큐로 선언할 것 인지 여부 (내구성)
            // => 해당 큐에 대해 내구성을 선택하면 수정할 수 없음
            // => 이건 보내는곳(publish)과 받는곳(consumer)가 동일해야함
            boolean durable = true;
            channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);
            for(int i=0; i<=1000; i++){
                String message = "Hello World => " + i;

                // props : 라우팅 헤더 등 메시지의 다른 속성들
                // => MessageProperties.PERSISTENT_TEXT_PLAIN 지속성
                channel.basicPublish("",
                        TASK_QUEUE_NAME,
                        MessageProperties.PERSISTENT_TEXT_PLAIN,
                        message.getBytes(StandardCharsets.UTF_8));
                System.out.println("[taskQueue] SEND message = " + message);
            }
        }
    }

    public static void fanoutQueue(ConnectionFactory factory) throws Exception{
        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {

            // exchangeDeclare : exchange 생성
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            String queueName = channel.queueDeclare().getQueue();
            // binding : exchange와 Queue 사이의 관계
            channel.queueBind(queueName, EXCHANGE_NAME, "");

            String message = "Hello World !";

            // props : 라우팅 헤더 등 메시지의 다른 속성들
            // => MessageProperties.PERSISTENT_TEXT_PLAIN 지속성
            channel.basicPublish(EXCHANGE_NAME,
                    "",
                    null,
                    message.getBytes(StandardCharsets.UTF_8));
            System.out.println("[fanoutQueue] SEND message = " + message);
        }
    }

    public static void directQueue(ConnectionFactory factory) throws Exception{
        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {

            // direct exchange => bindingKey가 routingKey와 일치해야함
            channel.exchangeDeclare(DIRECT_EXCHANGE_NAME, "direct");
            String queueName = channel.queueDeclare().getQueue();

            // binding : exchange와 Queue 사이의 관계
            channel.queueBind(queueName, DIRECT_EXCHANGE_NAME, "black");

            String message = "Hello World !";

            // props : 라우팅 헤더 등 메시지의 다른 속성들
            // => MessageProperties.PERSISTENT_TEXT_PLAIN 지속성
            channel.basicPublish(DIRECT_EXCHANGE_NAME,
                    "black",
                    null,
                    message.getBytes(StandardCharsets.UTF_8));
            System.out.println("[directQueue] SEND message = " + message);

        }
    }

    public static void topicQueue(ConnectionFactory factory) throws Exception{
        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {

            channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, "topic");
            String queueName = channel.queueDeclare().getQueue();

            String[] rk = new String[]{"aaa", "aaa.critical", "critical.test", "kern.merod", "test.ads.critical", "kern.critical"};
            for(String route : rk){
                System.out.println("route = " + route);
//                channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, route);

                String message = "Hello World ! " + route;

                // props : 라우팅 헤더 등 메시지의 다른 속성들
                // => MessageProperties.PERSISTENT_TEXT_PLAIN 지속성
                channel.basicPublish(TOPIC_EXCHANGE_NAME,
                        route,
                        null,
                        message.getBytes(StandardCharsets.UTF_8));
                System.out.println("[topicQueue] SEND message = " + message);
            }


        }
    }

}
