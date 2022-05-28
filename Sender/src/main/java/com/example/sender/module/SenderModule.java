package com.example.sender.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class SenderModule {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws Exception {
        System.out.println("주기적 호출");
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        try(Connection con = factory.newConnection(); Channel channel = con.createChannel()) {
            for(int i=0; i<=1000; i++){
                /** queueDeclare : Queue를 선언하는 메서드
                 * channel.queueDeclare(String queue, boolean durable, boolean exclusive,
                 *          boolean autoDelete, Map<String,Object> arguments)
                 * queue : 큐 이름
                 * durable : 서버 재시작에도 살아남을 튼튼한 큐로 선언할 것 인지 여부
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
                 * channel.basicPublish(String ecchange, String routingKey, boolean mandatory,
                 *          AMQP.BasicProperties props, byte[] body)
                 * exchange : 메시지를 발행하는 거래소
                 * routingKey : 라우팅 키
                 * mandatory : 필수 여부
                 * props : 라우팅 헤더 등 메시지의 다른 속성들
                 * body : 메시지 본문
                 */
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                System.out.println("SEND message = " + message);

                Thread.sleep(10);
            }
        }
    }
}
