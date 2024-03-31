package spyra.lukasz.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import spyra.lukasz.kafka.consumer.KafkaConsumer;
import spyra.lukasz.kafka.producer.KafkaProducer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@SpringBootTest(classes = KafkaIntroApplication.class)
@EmbeddedKafka(partitions = 2, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
class KafkaIntroApplicationTests {

  @ClassRule
  public static EmbeddedKafkaBroker embeddedKafka = new EmbeddedKafkaZKBroker(1, true, "multitype");

  @Autowired
  private KafkaProducer producer;

  @Autowired
  private KafkaConsumer consumer;

  @Test
  public void givenTopic_andConsumerGroup_whenConsumersListenToEvents_thenConsumeItCorrectly() throws Exception {
    Thread.sleep(1000);
    producer.sendMessagesToTopic1("hello-from-topic1 #", 10000);
    producer.sendMessagesToTopic2("hello-from-topic2 #", 10000);
    Thread.sleep(15000);

    final ConcurrentMap<String, ConcurrentMap<Integer, ConcurrentLinkedQueue<String>>> messagesFooByTopicAndPartition = groupMessagesByPartition(consumer.consumerFooGroupMessages);
    final ConcurrentMap<String, ConcurrentMap<Integer, ConcurrentLinkedQueue<String>>> messagesBarByTopicAndPartition = groupMessagesByPartition(consumer.consumerBarGroupMessages);

    System.out.println("Test complete");

  }

  private ConcurrentMap<String, ConcurrentMap<Integer, ConcurrentLinkedQueue<String>>> groupMessagesByPartition(final Map<String, ConcurrentLinkedQueue<ConsumerRecord<String, String>>> consumerGroupRecords) {
    return consumerGroupRecords
        .values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.groupingByConcurrent(ConsumerRecord::topic,
            Collectors.groupingByConcurrent(ConsumerRecord::partition,
                Collectors.mapping(
                    ConsumerRecord::value,
                    Collectors.toCollection(ConcurrentLinkedQueue::new)))));
  }

}
