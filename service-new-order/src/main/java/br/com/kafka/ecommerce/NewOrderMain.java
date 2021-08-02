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
                var _email = Math.random()+ "@email.com";

                for (var i = 0; i < 10; i++) {
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                    var order = new Order(orderId, amount, _email);
                    orderDispatch.send("ECOMMERCE_NEW_ORDER", _email, order);

                    String message = "Thank you for ypur Order! We are processing your order!";
                    var email = new Email(orderId, message,_email, message);
                    emailKafkaDispatch.send("ECOMMERCE_SEND_EMAIL",_email,  email);
                }
            }
        }
    }



}
