package br.com.kafka.ecommerce.service;

import br.com.kafka.ecommerce.models.Email;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL", emailService::parse,
                Email.class, new HashMap<String, String>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Email> record) {
        System.out.println("::::_________________________________________________::::");
        System.out.println("Send Email! ");
        System.out.println("Chave " + record.key());
        System.out.println("Value " + record.value());
        System.out.println("Partition " + record.partition());
        System.out.println("Offset " + record.offset());
    }

}
