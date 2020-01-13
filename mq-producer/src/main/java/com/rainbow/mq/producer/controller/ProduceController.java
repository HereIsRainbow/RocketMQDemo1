package com.rainbow.mq.producer.controller;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;

import java.util.List;

@RestController
@RequestMapping("/produce")
public class ProduceController {
    @Resource
    private DefaultMQProducer defaultMQProducer;
    //设置topic,tag值
    @Value("${mq.producer.topic}")
    private String topic;
    @Value("${mq.producer.tag}")
    private String tag;

    //接收到一个消息
    @RequestMapping
    public Object product(String message) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        List<Message> msgList = new ArrayList<>();
        //循环给数组添加信息
        for (int i = 0;i<10;i++){
            msgList.add(new Message(topic,tag,("这是第"+i+"条数据").getBytes()));
        }

        //send发送对象是Message,也可以是Collection<Message>,抛异常
        SendResult send = defaultMQProducer.send(msgList);
        return "发送成功";
    }

    //=================

    //消费者
    //指定nameserver
    //创建消费者，指定消费者组
    //设置消费者，信息偏移量

}
