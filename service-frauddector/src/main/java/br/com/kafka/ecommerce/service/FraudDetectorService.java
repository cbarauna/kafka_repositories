package br.com.kafka.ecommerce.service;

import br.com.kafka.ecommerce.models.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class FraudDetectorService {
    public static void main(String[] args) {

        var fraudService = new FraudDetectorService();

         try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
                fraudService::parse, Order.class, new HashMap<String, String>())){
             service.run();
         }
    }

    private void parse(ConsumerRecord<String, Order> record) {
        System.out.println("::::_________________________________________________::::");
        System.out.println("Processsando new Order, checking for fraud ");
        System.out.println("Chave " + record.key());
        System.out.println("Value " + record.value());
        System.out.println("Partition " + record.partition());
        System.out.println("Offset " + record.offset());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e){
                e.printStackTrace();
            }
        System.out.println("Order Processed");
    }

}
