package se.cag.labs.order.producer;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "kafka")
@Data
public class OrderProducerConfiguration {
    private String servers;
    private String topic;
    private int replication;
    private int partitions;

}
