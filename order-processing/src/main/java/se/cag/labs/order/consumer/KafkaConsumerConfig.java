package se.cag.labs.order.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {
  private final OrderProcessorConfiguration orderProcessorConfiguration;

  public KafkaConsumerConfig(OrderProcessorConfiguration orderProcessorConfiguration) {
    this.orderProcessorConfiguration = orderProcessorConfiguration;
  }

  @Bean
  public ConsumerFactory<String, String> consumerFactory() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
      orderProcessorConfiguration.getServers());
    configProps.put(
      ConsumerConfig.GROUP_ID_CONFIG,
      StringDeserializer.class);
    configProps.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class);
    configProps.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      StringDeserializer.class);
    configProps.put(
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
      "latest");
    return new DefaultKafkaConsumerFactory<>(configProps) {
    };
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());

    return factory;
  }
}
