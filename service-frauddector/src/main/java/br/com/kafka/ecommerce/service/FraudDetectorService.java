package br.com.kafka.ecommerce.service;

import br.com.kafka.ecommerce.KafkaDispatch;
import br.com.kafka.ecommerce.models.Order;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {
    public static void main(String[] args) {

        var fraudService = new FraudDetectorService();

        try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
                fraudService::parse, Order.class, new HashMap<String, String>())) {
            service.run();
        }
    }

    private final KafkaDispatch<Order> orderDispatch = new KafkaDispatch<>();

    private void parse(ConsumerRecord<String, Order> record) throws ExecutionException, InterruptedException {
        System.out.println("::::_________________________________________________::::");
        System.out.println("Processsando new Order, checking for fraud ");
        System.out.println("Chave " + record.key());
        System.out.println("Value " + record.value());
        System.out.println("Partition " + record.partition());
        System.out.println("Offset " + record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        var order = record.value();
        if (isFraud(order)) {
            System.out.println("Order is a Fraud!!! " + new Gson().toJson(order));
            orderDispatch.send("ECOMMERCE_ORDER_REJECT", order.getEmail(), order);
        } else {
            System.out.println("Approved: " + new Gson().toJson(order));
            orderDispatch.send("ECOMMERCE_ORDER_APROVED", order.getEmail(), order);
        }
        System.out.println("Order Processed");
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
