package com.rainbow.mq.producer.config;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.nio.charset.Charset;
import java.util.List;

@Configuration
//该注解指定特定的配置文件
@PropertySource("classpath:mq.properties")
public class MqConsumerConfig {
    //引入的是mq.properties配置文件中的内容
    @Value("${consumer.nameServer}")
    private String nameServer;
    @Value("${consumer.consumerGroup}")
    private String consumerGroup;
    @Value("${consumer.topic}")
    private String topic;

    //pull:broker角度：consumer主动拉取消息
    //push：broker角度：broker主动推消息
    //建议使用pull,将broker的压力分摊到consumer中,但是push的底层是由pull完成的
    //所以使用push的方法
    @Bean
    public Object DefaultMQPullConsumer() throws MQClientException {
        //设置主动拉取的Consumer
//        DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer();
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer();
        defaultMQPushConsumer.setNamesrvAddr(this.nameServer);
        defaultMQPushConsumer.setConsumerGroup(this.consumerGroup);
        //消费消息
        //1.设置消费者从哪个地方开始消费，从上一次消费的地方开始
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        //2.订阅，只支持或者的方式
        defaultMQPushConsumer.subscribe(this.topic,"MyTag");
        //3.注册消息监听器(两种order,concurrent，没有太大区别)
        defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                //查看一下并发的情况,获取线程名称
                System.out.println(Thread.currentThread().getName());
                //List<MessgeExt>msgs 接收到的消息， ConsumeConcurrentlyContext解决并发的问题
                msgs.forEach(msg ->{
                    //获取消息体,并指定编码格式（defaultCharset默认编码模式是utf-8）
                    String message = new String(msg.getBody(),Charset.defaultCharset());
                    System.out.println(message);
                });
                //返回监听事务的状态枚举值
                //Fixme  ConsumeConcurrentlyStatus.RECONSUME_LATER消息的重复消费,CONSUME_SUCCESS消费成功；
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        //消费者启动
        defaultMQPushConsumer.start();
        return defaultMQPushConsumer;
    }
}
