package br.com.kafka.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface CosumerFunction<T> {

    void consume(ConsumerRecord<String, T> record);
}
