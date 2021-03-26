package se.cag.labs.order.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import se.cag.labs.kafka.model.Order;

@RestController
@Slf4j
public class OrderProducerResource {

    private OrderProducerConfiguration configuration;

    private ObjectMapper mapper = new ObjectMapper();

    public OrderProducerResource(OrderProducerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("orders/order")
    public void produceOrder(@RequestBody Order order) throws JsonProcessingException {
        log.info(order.toString());
        kafkaTemplate.send(configuration.getTopic(), mapper.writeValueAsString(order));
    }

}
