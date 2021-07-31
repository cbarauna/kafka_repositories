package br.com.kafka.ecommerce;

import br.com.kafka.ecommerce.models.Email;
import br.com.kafka.ecommerce.models.Order;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatch = new KafkaDispatch<Order>()) {
            try(var emailKafkaDispatch = new KafkaDispatch<Email>()) {

                for (var i = 0; i < 1000; i++) {
                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                    var order = new Order(userId, orderId, amount);
                    orderDispatch.send("ECOMMERCE_NEW_ORDER", userId, order);

                    String message = "Thank you for ypur Order! We are processing your order!";
                    var email = new Email(userId,message,userId,message);
                    emailKafkaDispatch.send("ECOMMERCE_SEND_EMAIL", userId, email);
                }
            }
        }
    }



}
