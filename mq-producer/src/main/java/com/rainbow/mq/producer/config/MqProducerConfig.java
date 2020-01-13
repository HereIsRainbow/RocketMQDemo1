package com.rainbow.mq.producer.config;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;




@Configuration
//这个注解修饰类/方法，但是一定要是@Configuration修饰的类里面
@ConfigurationProperties(prefix = "mq.producer")
public class MqProducerConfig {

    private String nameServer;
    private String producerGroup;

    //配置文件的前缀要与注解中的一致
    //@ConfigurationProperties到配置文件中，将以上所有属性配置一下,设置get/set方法，配置文件的属性值就会赋值给该类的成员属性

    public String getNameServer() {
        return nameServer;
    }

    public void setNameServer(String nameServer) {
        this.nameServer = nameServer;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }



    //创建的生产者，属于上面定义的生产者组
    @Bean
    public DefaultMQProducer defaultMQProducer(){
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer();
        //设置生产者的nameserver地址，所属组
        defaultMQProducer.setNamesrvAddr(this.nameServer);
        defaultMQProducer.setProducerGroup(this.producerGroup);
        try {
            //调用start方法，启动生产者，捕获异常
            defaultMQProducer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
        return defaultMQProducer;
    }
}
