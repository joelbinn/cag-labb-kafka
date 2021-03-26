package se.cag.labs.order.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import se.cag.labs.kafka.model.Order;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

  private final OrderProcessorConfiguration orderProcessorConfiguration;
  private final ObjectMapper objectMapper;

  public KafkaStreamsConfig(OrderProcessorConfiguration orderProcessorConfiguration, ObjectMapper objectMapper) {
    this.orderProcessorConfiguration = orderProcessorConfiguration;
    this.objectMapper = objectMapper;
  }

  @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
  public KafkaStreamsConfiguration kStreamsConfigs() {
    return new KafkaStreamsConfiguration(
      Map.of(
        StreamsConfig.APPLICATION_ID_CONFIG, "order-processiong-stream-jobi",
        StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, orderProcessorConfiguration.getServers(),
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
      )
    );
  }

  @Bean
  public ProducerFactory<String, String> producerFactory() {
    Map<String, Object> configProps = new HashMap<>();
    configProps.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      orderProcessorConfiguration.getServers());
    configProps.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      StringSerializer.class);
    configProps.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      StringSerializer.class);
    return new DefaultKafkaProducerFactory<>(configProps);
  }

  @Bean
  public KafkaTemplate<String, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  @Bean
  public KafkaAdmin kafkaAdmin() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, orderProcessorConfiguration.getServers());
    return new KafkaAdmin(configs);
  }

  @Bean
  public NewTopic backorderTopic() {
    return new NewTopic(orderProcessorConfiguration.getBackorderTopic(), 1, (short) 1);
  }

  @Bean
  public NewTopic packTopic() {
    return new NewTopic(orderProcessorConfiguration.getPackTopic(), 1, (short) 1);
  }


  @Bean
  public KStream<String, String>[] kStream(StreamsBuilder streamsBuilder) {
    val stream = streamsBuilder.stream(
      orderProcessorConfiguration.getInTopic(),
      Consumed.with(Serdes.String(), Serdes.String())
    );
    val streams = stream
      .branch(
        (k, v) -> toOrder(v).getOrderNo() < 10,
        (k, v) -> toOrder(v).getOrderNo() >= 10
      );

    streams[0].to(orderProcessorConfiguration.getBackorderTopic());
    streams[1].to(orderProcessorConfiguration.getPackTopic());

    return streams;
  }

  private Order toOrder(Object v) {
    try {
      return objectMapper.readValue((String) v, Order.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
