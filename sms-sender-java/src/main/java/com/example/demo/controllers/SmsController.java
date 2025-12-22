package com.example.demo.controllers;

import java.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.example.demo.models.SmsRequest;

@RestController
@RequestMapping("/v1/sms")
public class SmsController {
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("/send")
    public ResponseEntity<String> sendSms(@RequestBody SmsRequest request) {
        Boolean isBlocked = redisTemplate.hasKey("blocked:"+request.getPhoneNumber());
        if(isBlocked != null && isBlocked) {
            return ResponseEntity.status(403).body("SMS FAILED: the User is in the blockList");
        }

        double randomNum = Math.random();
        System.out.println("randomNumber: " + randomNum);

        String status = (randomNum > 0.5)? "SUCCESS": "FAILURE";

        String KafkaMessage = String.format(
            "{\"phoneNumber\":\"%s\", \"message\":\"%s\", \"status\":\"%s\"}",
            request.getPhoneNumber(),
            request.getMessage(),
            status
        );
        kafkaTemplate.send("sms-log-topic", KafkaMessage);

        return ResponseEntity.ok("SMS processed. Status: " + status);
    };

    @PostMapping("/block")
    public ResponseEntity<String> blockUser(@RequestBody SmsRequest request) {
        redisTemplate.opsForValue().set("blocked:"+request.getPhoneNumber(), "true");
        return ResponseEntity.ok("User with phone number " + request.getPhoneNumber() + " has been blocked.");
    };

    @GetMapping("/getAllBlocked")
    public ResponseEntity<String> getAllBlockedUsers() {
        Set<String> keys = redisTemplate.keys("blocked:*");
        StringBuilder blockedUsers = new StringBuilder("Blocked Users:\n");
        if(keys == null) return ResponseEntity.ok("No blocked users found.");
        for(String key : keys) {
            String phoneNumber = key.replace("blocked:", "");
            blockedUsers.append(phoneNumber).append("\n");
        }
        return ResponseEntity.ok(blockedUsers.toString());
    };

    @PostMapping("/unblock")
    public ResponseEntity<String> unblockUser(@RequestBody SmsRequest request) {
        redisTemplate.delete("blocked:"+request.getPhoneNumber());
        return ResponseEntity.ok("User with phone number " + request.getPhoneNumber() + " has been unblocked.");
    };
}
