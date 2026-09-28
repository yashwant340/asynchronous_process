package com.ds.lab1.consumer.service; // <- must match the folder you put this file in

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
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

/**
 * The whole consumer in one class.
 *
 * Flow for every message that appears on task_queue:
 *   1. listen()          receives {"id": "...", "text": "...", "timestamp": "..."}
 *   2. callOllama()      sends the text to the AI and gets the answer
 *   3. sendToProducer()  POSTs the answer to the producer's /result/{id}
 *   4. basicAck          tells RabbitMQ "finished" -- only AFTER 2 and 3
 */
@Service
public class ConsumerService {

    private static final String OLLAMA_URL = "http://localhost:11434/api/generate";
    private static final String OLLAMA_MODEL = "llama3.2:1b";

    private final ObjectMapper mapper = new ObjectMapper();
    private final RestTemplate restTemplate = createRestTemplate();


    private String producerResultUrl = "http://localhost:8080/api/result/{id}";

    // ---------------------------------------------------------------
    // 1. LISTEN to the queue
    // ---------------------------------------------------------------
    @RabbitListener(queues = "task_queue")
    public void listen(Message message,
                       Channel channel,
                       @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        // The message body is JSON text. Pull out the two fields we need.
        JsonNode json = mapper.readTree(message.getBody());
        String id = json.get("id").asText();
        String text = json.get("text").asText();
        System.out.println("[consumer] received " + id + ": " + text);

        // 2. CALL OLLAMA. The producer treats the exact word "Error"
        // as the failure marker, so that is what we send if the AI fails.
        String answer;
        try {
            answer = callOllama(text);
        } catch (Exception e) {
            System.out.println("[consumer] Ollama failed for " + id + ": " + e.getMessage());
            answer = "Error";
        }

        // 3. RETURN the answer to the producer
        try {
            sendToProducer(id, answer);
            System.out.println("[consumer] sent result for " + id);
        } catch (Exception e) {
            System.out.println("[consumer] could not reach producer for " + id + ": " + e.getMessage());
        }

        // 4. ACK -- we are done with this message
        channel.basicAck(deliveryTag, false);
    }

    // ---------------------------------------------------------------
    // 2. Call Ollama
    // ---------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private String callOllama(String text) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> body = Map.of(
                "model", OLLAMA_MODEL,
                "prompt", text,
                "stream", false   // one complete answer, not word-by-word
        );

        Map<String, Object> response = restTemplate.postForObject(
                OLLAMA_URL, new HttpEntity<>(body, headers), Map.class);

        if (response == null || response.get("response") == null) {
            throw new RuntimeException("Ollama returned no answer");
        }
        return response.get("response").toString();
    }

    // ---------------------------------------------------------------
    // 3. Send the result back to the producer
    // ---------------------------------------------------------------
    private void sendToProducer(String id, String result) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        // "{id}" in the URL is replaced by the id argument
        restTemplate.postForEntity(producerResultUrl, new HttpEntity<>(result, headers), Void.class, id);
    }

    // Stop waiting on the AI after 60s instead of hanging forever
    // (the first call can be slow while the model loads).
    private static RestTemplate createRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(5000);
        factory.setReadTimeout(60000);
        return new RestTemplate(factory);
    }
}
