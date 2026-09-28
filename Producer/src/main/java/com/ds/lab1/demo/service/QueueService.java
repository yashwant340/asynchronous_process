package com.ds.lab1.demo.service;

import com.ds.lab1.demo.dto.Message;
import com.ds.lab1.demo.dto.ProcessResponseDTO;
import com.ds.lab1.demo.wrapper.MessageWrapper;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class QueueService {

    private final RabbitTemplate rabbitTemplate;

    private final Map<String, String> results = new ConcurrentHashMap<>();

    public QueueService(RabbitTemplate rabbitTemplate){
        this.rabbitTemplate = rabbitTemplate;
    }

    public ProcessResponseDTO processQueue(MessageWrapper message) {

        String requestId = UUID.randomUUID().toString();

        String now = Timestamp.from(Instant.now()).toString();

        Message task = new Message(requestId, message.getText(), now);
        System.out.println("Sending request to message queue: " + task.getId() + " " + task.getText());
        rabbitTemplate.convertAndSend(
                "task_queue",
                task,
                msg -> {
                    msg.getMessageProperties()
                            .setDeliveryMode(MessageDeliveryMode.PERSISTENT);
                    return msg;
                }
        );
        results.put(requestId, "");

        return new ProcessResponseDTO(requestId);
    }

    public String getResult(String id) {
        System.out.println("Getting result for: " + id);
        return results.get(id).equals("Error")? "Error" : results.get(id).isEmpty() ? "Processing" : "Completed";
    }

    public void storeResult(String id, String result) {
        System.out.println("Storing result for: " + id + " result:" + result);
        results.put(id, result);
    }
}
