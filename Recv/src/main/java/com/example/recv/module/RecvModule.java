package com.example.recv.module;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;

public class RecvModule {
    private final static String QUEUE_NAME = "hello";
    private final static String TASK_QUEUE_NAME = "task_queue";
    private static final String EXCHANGE_NAME = "logs";
    private static final String DIRECT_EXCHANGE_NAME = "logsDirect";
    private static final String TOPIC_EXCHANGE_NAME = "logsTopic";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.35.35");
        factory.setPort(5672);
        factory.setUsername("root");
        factory.setPassword("test123#");

        // Connection : Application과 RabbitMQ broker 사이의 TCP 연결
        Connection connection = factory.newConnection();
        // Channel : connection 내부에 정의된 가상 연결. Queue에서 데이터를 손볼 때 생기는 일종의 통로같은 개념
        Channel channel = connection.createChannel();

//        helloConsumer(channel);
//        taskConsumer(channel);
//        fanoutConsumer(channel);
        directConsumer(channel);
        directConsumer2(channel);
        topicConsumer(channel);
    }

    private static void helloConsumer(Channel channel) throws Exception {
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
        System.out.println(" [helloConsumer] Waiting for messages. To exit press CTRL+C");

        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[helloConsumer] RECV message = " + message);
            try{
                doWork(message);
            } catch(Exception e){
                System.out.println(e.getMessage());
            } finally {
                System.out.println("[helloConsumer] Done Finally");
                // BassicAck 메소드를 사용하여 메세지 처리가 완료되면 Queue에서 지우도록 Ack 전송
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        });

        // Queue에서 Consumer로 메시지 전송이 완료되는 순간 Consumer가 Queue에게 승인을 넘기고 Queue는 승인을 받아야만
        // 다음 데이터를 Consumer에게 넘겨준다.
        // 그래서 승인이 중요하다.. 승인을 안해주면 Consumer가 멈추게 된다.
        // 문제는 Consumer가 승인을 보내지않고 커넥션을 끊은 후 다시 접속하면
        // 데이터는 복구되기 위해 큐에 다시 적재되는데 이경우 이미 처리한 데이터를 또 처리하게 된다. 그래서 반드시 승인해줘야한다.
        boolean autoAck = false;
        channel.basicConsume(QUEUE_NAME, autoAck, dc, consumerTag -> {});
    }

    private static void taskConsumer(Channel channel) throws Exception {
        boolean durable = true;
        channel.queueDeclare(TASK_QUEUE_NAME, durable, false, false, null);

        System.out.println(" [taskConsumer] Waiting for messages. To exit press CTRL+C");
        // Queue에 업무를 할당하다보면 한쪽에 몰릴수가있다.
        // 그것을 방지하기 위해 prefetchCount = 1; 설정으로 작업자에게 한번에 하나 이상의 메시지를
        // 주지 않도록 RabbitMQ에 지시한다.
        // 즉, 이전 메시지를 처리하고 승인할 때 까지 작업자에게 새 메시지를 발송하지 않고 다른 작업자에게 전송
        int prefetchCount = 1;
        channel.basicQos(prefetchCount);


        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[taskConsumer] RECV message = " + message);
            try{
                doWork(message);
            } catch(Exception e){
                System.out.println(e.getMessage());
            } finally {
                System.out.println("[taskConsumer] Done Finally");
                // BassicAck 메소드를 사용하여 메세지 처리가 완료되면 Queue에서 지우도록 Ack 전송
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        });

        boolean autoAck = false;
        channel.basicConsume(TASK_QUEUE_NAME, autoAck, dc, consumerTag -> {});
    }

    private static void fanoutConsumer(Channel channel) throws Exception {
        // exchangeDeclare : exchange 생성
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
        String queueName = channel.queueDeclare().getQueue();

        // binding : exchange와 Queue 사이의 관계
        // binding은 추가 routeKey 매개 변수를 사용할 수 있다.=> 이것을 bindingkey(routingKey)라고 함
        // fanout은 routingKey을 무시
        channel.queueBind(queueName, EXCHANGE_NAME, "");
        System.out.println("[fanoutConsumer] Waiting for messages. To exit press CTRL+C");

        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[fanoutConsumer] RECV message = " + message);
        });

        channel.basicConsume(queueName, true, dc, consumerTag -> {});
    }

    private static void directConsumer(Channel channel) throws Exception {
        // exchangeDeclare : exchange 생성
        channel.exchangeDeclare(DIRECT_EXCHANGE_NAME, "direct");
        String queueName = channel.queueDeclare().getQueue();

        // binding은 추가 routeKey 매개 변수를 사용할 수 있다.=> 이것을 bindingkey(routingKey)라고 함
        // key로 바인딩을 만드는 법법
       channel.queueBind(queueName, DIRECT_EXCHANGE_NAME, "black");
        System.out.println("[directConsumer] Waiting for messages. To exit press CTRL+C");

        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[directConsumer] RECV routingKey = "+ delivery.getEnvelope().getRoutingKey() +" : message = " + message);
        });

        channel.basicConsume(queueName, true, dc, consumerTag -> {});
    }

    private static void directConsumer2(Channel channel) throws Exception {
        // exchangeDeclare : exchange 생성
        channel.exchangeDeclare(DIRECT_EXCHANGE_NAME, "direct");
        String queueName = channel.queueDeclare().getQueue();

        // binding은 추가 routeKey 매개 변수를 사용할 수 있다.=> 이것을 bindingkey(routingKey)라고 함
        // key로 바인딩을 만드는 법법
       channel.queueBind(queueName, DIRECT_EXCHANGE_NAME, "orange");
        System.out.println("[directConsumer2] Waiting for messages. To exit press CTRL+C");

        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[directConsumer2] RECV routingKey = "+ delivery.getEnvelope().getRoutingKey() +" : message = " + message);
        });

        channel.basicConsume(queueName, true, dc, consumerTag -> {});
    }

    private static void topicConsumer(Channel channel) throws Exception {
        channel.exchangeDeclare(TOPIC_EXCHANGE_NAME, "topic");
        String queueName = channel.queueDeclare().getQueue();

        String[] rk = new String[]{"kern.*", "*.critical", "kern.critical"};
        for(String route : rk){
            System.out.println("[topicConsumer] route => " + route);
            channel.queueBind(queueName, TOPIC_EXCHANGE_NAME, route);
        }
        System.out.println("[topicConsumer] Waiting for messages. To exit press CTRL+C");

        DeliverCallback dc = ((consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[topicConsumer] RECV routingKey = "+ delivery.getEnvelope().getRoutingKey() +" : message = " + message);
        });

        channel.basicConsume(queueName, true, dc, consumerTag -> {});
    }

    private static void doWork(String task) {
        for (char ch : task.toCharArray()) {
            if (ch == '.') {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException _ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
