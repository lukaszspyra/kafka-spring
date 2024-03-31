package spyra.lukasz.kafka.producer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

@Service
public class KafkaProducer {

  private final KafkaTemplate<String, String> kafkaTemplate;

  @Value(value = "${spring.kafka.topic1.name}")
  private String topic1Name;

  @Value(value = "${spring.kafka.topic2.name}")
  private String topic2Name;

  private final Map<String, ConcurrentLinkedQueue<String>> producedMessages = new ConcurrentHashMap<>();

  public KafkaProducer(final KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  /**
   * Produces 50 messages to topic1, which name is defined in properties
   * Uses callback whenComplete() for async operation (for sync see get() from CompletableFuture)
   *
   * @param msg
   */
  public void sendMessagesToTopic1(String msg, int quantity) {
    IntStream.rangeClosed(1, quantity).forEach(
        i -> kafkaTemplate.send(topic1Name, msg + i).whenComplete(collectMessage(msg + i))
    );
  }

  public void sendMessagesToTopic2(String msg, int quantity) {
    IntStream.rangeClosed(1, quantity).forEach(
        i -> kafkaTemplate.send(topic2Name, msg + i).whenComplete(collectMessage(msg + i))
    );
  }

  private BiConsumer<SendResult<String, String>, Throwable> collectMessage(final String msg) {
    return (result, ex) -> producedMessages.merge(result.getProducerRecord().topic(), new ConcurrentLinkedQueue<>(),
        (oldQueue, newQueue) -> {
          newQueue.offer(msg);
          return newQueue;
        });
  }

  public void printAllProducedMessages() {
    producedMessages.forEach((k, v) -> System.out.println("[" + k + "]=[" + v + "]"));
  }

}
