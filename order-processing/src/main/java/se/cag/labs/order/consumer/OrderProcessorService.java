package se.cag.labs.order.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

/**
 * Service that in
 * Iteration 1: Listens to your order kafka topic and prints the orders on stdout...
 * Iteration 2: Streams Kafka topic and sends orders to 2 distinct Sink:s.
 */
@Service
@Slf4j
public class OrderProcessorService {

    private final OrderProcessorConfiguration configuration;
    private final ConsumerFactory<String, String> consumerFactory;

    public OrderProcessorService(OrderProcessorConfiguration configuration, ConsumerFactory<String, String> consumerFactory) {
        this.configuration = configuration;
        this.consumerFactory = consumerFactory;
    }

    @KafkaListener(topics = "${kafka.inTopic}", groupId = "${kafka.groupId}")
    public void listenOnTopic(String message) {
        try {
            System.out.println("Received message: " + message);
        } catch (Exception e) {
            System.exit(-1);
        }
    }

//    @Scheduled
//    public void poll() {
//        consumerFactory.createConsumer().poll(Duration.ofMillis(60*1000));
//    }
//
//    @KafkaListener(topics = "${kafka.inTopic}", groupId = "${kafka.groupId}")
//    public void listenOnTopic(List<String> messages, Acknowledgment ack) {
//        try {
//            System.out.println("Received message: " + messages);
//            ack.acknowledge();
//        } catch (Exception e) {
//            System.exit(-1);
//        }
//    }
}
