package com.ds.lab1.demo.controller;

import com.ds.lab1.demo.dto.ProcessResponseDTO;
import com.ds.lab1.demo.service.QueueService;
import com.ds.lab1.demo.wrapper.MessageWrapper;
import org.springframework.web.bind.annotation.*;

@RestController
public class PromptController {

    private final QueueService queueService;

    public PromptController(QueueService queueService) {
        this.queueService = queueService;
    }

    @PostMapping("/process")
    public ProcessResponseDTO processMessage(@RequestBody MessageWrapper message){
        return queueService.processQueue(message);
    }

    @GetMapping("/result/{id}")
    public String send(@PathVariable String id) {
        return queueService.getResult(id);
    }

    @PostMapping("/result/{id}")
    public void storeResult(@PathVariable String id, @RequestBody String result){
        queueService.storeResult(id, result);
    }
}