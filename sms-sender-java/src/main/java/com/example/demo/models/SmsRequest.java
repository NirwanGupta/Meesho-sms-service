package com.example.demo.models;

public class SmsRequest {
    private String phoneNumber;
    private String message;

    public String getPhoneNumber() {
        return this.phoneNumber;
    }
    public String getMessage() {
        return this.message;
    }
    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }
    public void setMessage(String message) {
        this.message = message;
    }
}