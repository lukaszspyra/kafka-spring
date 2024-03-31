package spyra.lukasz.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Service
public class KafkaConsumer {

  public Map<String, ConcurrentLinkedQueue<ConsumerRecord<String, String>>> consumerFooGroupMessages = new ConcurrentHashMap<>();
  public Map<String, ConcurrentLinkedQueue<ConsumerRecord<String, String>>> consumerBarGroupMessages = new ConcurrentHashMap<>();


  @KafkaListener(topics = {"topic1", "topic2"})
  public void listenGroupFoo(ConsumerRecord<String, String> consumerRecord) {
    collectConsumedMessage(consumerRecord, consumerFooGroupMessages);
  }

  @KafkaListener(topics = "topic1", groupId = "bar")
  public void listenGroupBar1(ConsumerRecord<String, String> consumerRecord) {
    collectConsumedMessage(consumerRecord, consumerBarGroupMessages);
  }

  @KafkaListener(topics = "topic1", groupId = "bar")
  public void listenGroupBar2(ConsumerRecord<String, String> consumerRecord) {
    collectConsumedMessage(consumerRecord, consumerBarGroupMessages);
  }

  private void collectConsumedMessage(final ConsumerRecord<String, String> consumerRecord, final Map<String, ConcurrentLinkedQueue<ConsumerRecord<String,String>>> groupMessages) {
    groupMessages.putIfAbsent(consumerRecord.topic(), new ConcurrentLinkedQueue<>());
    groupMessages.computeIfPresent(consumerRecord.topic(), (a, b) -> {
      b.offer(consumerRecord);
      return b;
    });
  }

}
