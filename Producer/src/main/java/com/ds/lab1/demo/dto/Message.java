package com.ds.lab1.demo.dto;


import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Message {

    private String id;
    private String text;
    private String timestamp;

}
