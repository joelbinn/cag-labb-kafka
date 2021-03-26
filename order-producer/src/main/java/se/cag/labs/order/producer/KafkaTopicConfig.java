/*
 * User: joel
 * Date: 2021-03-26
 * Time: 10:54
 */
package se.cag.labs.order.producer;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {
  private final OrderProducerConfiguration orderProducerConfiguration;

  public KafkaTopicConfig(OrderProducerConfiguration orderProducerConfiguration) {
    this.orderProducerConfiguration = orderProducerConfiguration;
  }

  @Bean
  public KafkaAdmin kafkaAdmin() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, orderProducerConfiguration.getServers());
    return new KafkaAdmin(configs);
  }

  @Bean
  public NewTopic topic1() {
    return new NewTopic(orderProducerConfiguration.getTopic(), 1, (short) 1);
  }


}
