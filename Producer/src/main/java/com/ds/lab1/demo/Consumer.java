package com.ds.lab1.demo;

import com.ds.lab1.demo.dto.Message;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class Consumer {

    @RabbitListener(queues="task_queue")
    public void receiveMessage(
            Message message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws Exception {

        System.out.println("Processing message: " + message.getId());
        System.out.println("Text: " + message.getText());

        // Simulate processing
        Thread.sleep(3000);


        // This is never reached
         channel.basicAck(deliveryTag, false);
    }
}
