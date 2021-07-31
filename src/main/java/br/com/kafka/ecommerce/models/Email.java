package br.com.kafka.ecommerce.models;

public class Email {
    private final String key;
    private final String message;
    private final String subject;
    private final String body;

    public Email(String key, String message, String subject, String body) {
        this.key = key;
        this.message = message;
        this.subject = subject;
        this.body = body;
    }
}
