package com.ds.lab1.consumer.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Map;


@Service
public class ConsumerService {

    private static final String RETRY_COUNT_HEADER = "x-retry-count";
    private static final int MAX_RETRIES = 2;

    private static final String OLLAMA_URL = "http://localhost:11435/api/generate";
    private static final String OLLAMA_MODEL = "llama3.2:1b";

    private final ObjectMapper mapper = new ObjectMapper();
    private final RestTemplate restTemplate = createRestTemplate();
    private final RabbitTemplate rabbitTemplate;

    public ConsumerService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    private String producerResultUrl = "http://localhost:8080/result/{id}";

    @RabbitListener(queues = "task_queue")
    public void listen(Message message,
                       Channel channel,
                       @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        JsonNode json = mapper.readTree(message.getBody());
        String id = json.get("id").asText();
        String text = json.get("text").asText();

        String answer;
        try {
            System.out.println("Processing request:" + id + " " + text);
            Thread.sleep(3000);
            answer = callOllama(text);

            sendToProducer(id, answer);

            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {

            Object retryHeader = message.getMessageProperties().getHeaders().get(RETRY_COUNT_HEADER);
            int retryCount = retryHeader instanceof Number number ? number.intValue() : 0;
            System.out.println("Retrying the following request:" + message +"Retry count for:" + retryCount);
            if (retryCount < MAX_RETRIES) {
                Message retryMessage = MessageBuilder.fromMessage(message)
                        .setHeader(RETRY_COUNT_HEADER, retryCount + 1)
                        .build();
                rabbitTemplate.send("task_queue", retryMessage);
                channel.basicAck(deliveryTag, false);

            } else {
                try {
                    System.out.println("Max retries completed for request:" + message);
                    sendToProducer(id, "Error");
                } catch (Exception callbackFailure) {
                    System.out.println("[consumer] could not report final failure for " + id
                            + ": " + callbackFailure.getMessage());
                }

                channel.basicNack(deliveryTag, false, false);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private String callOllama(String text) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> body = Map.of(
                "model", OLLAMA_MODEL,
                "prompt", text,
                "stream", false
        );
        System.out.println("Calling ollama with prompt: " + body.get("prompt"));
        Map<String, Object> response = restTemplate.postForObject(
                OLLAMA_URL, new HttpEntity<>(body, headers), Map.class);

        if (response == null || response.get("response") == null) {
            System.out.println("Ollama returned no answer");
            throw new RuntimeException("Ollama returned no answer");
        }

        return response.get("response").toString();
    }


    private void sendToProducer(String id, String result) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);

        System.out.println("Sending result to producer: " + result);
        restTemplate.postForEntity(producerResultUrl, new HttpEntity<>(result, headers), Void.class, id);
    }


    private static RestTemplate createRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5000);
        factory.setReadTimeout(60000);
        return new RestTemplate(factory);
    }
}
