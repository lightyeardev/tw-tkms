package com.transferwise.kafka.tkms.test;

import com.transferwise.common.baseutils.ExceptionUtils;
import com.transferwise.kafka.tkms.test.TestProperties.KafkaServer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class KafkaConfiguration implements InitializingBean {

  @Autowired
  private KafkaAdmin kafkaAdmin;
  @Autowired
  private TestProperties tkmsProperties;

  @Override
  public void afterPropertiesSet() {
    for (var kafkaServer : tkmsProperties.getKafkaServers()) {
      var props = getKafkaProperties(kafkaServer);
      for (var testTopic : kafkaServer.getTopics()) {
        try (AdminClient adminClient = AdminClient.create(props)) {
          deleteTopic(adminClient, testTopic);
        }
        try (AdminClient adminClient = AdminClient.create(props)) {
          createTopic(adminClient, testTopic, 10);
        }
      }
    }
  }

  protected Map<String, Object> getKafkaProperties(KafkaServer kafkaServer) {
    var props = new HashMap<>(kafkaAdmin.getConfigurationProperties());
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer.getBootstrapServers());
    return props;
  }

  protected void deleteTopic(AdminClient adminClient, String topicName) {
    try {
      final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singleton(topicName));
      deleteTopicsResult.topicNameValues().get(topicName).get();

      log.info("Deleted Kafka topic '" + topicName + "'.");
    } catch (InterruptedException | ExecutionException e) {
      if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  @SuppressWarnings("SameParameterValue")
  protected void createTopic(final AdminClient adminClient, final String topicName, final int partitions) {
    // It sometimes takes time for delete to actually apply and finalize.
    for (int i = 0; i < 50; i++) {
      try {
        final NewTopic newTopic = new NewTopic(topicName, partitions, (short) 1);
        final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
        createTopicsResult.values().get(topicName).get();
        log.info("Created Kafka topic '" + topicName + "' in " + i + ".");
        awaitTopicMetadataPropagated(adminClient, topicName, partitions);
        return;
      } catch (InterruptedException | ExecutionException e) {
        if (!(e.getCause() instanceof TopicExistsException)) {
          throw new RuntimeException(e.getMessage(), e);
        }
        ExceptionUtils.doUnchecked(() -> Thread.sleep(5));
      }
    }
  }

  /**
   * `createTopics` acknowledges as soon as the topic is created in the controller, but partition leadership is not necessarily propagated to all
   * brokers yet. TKMS validates topics via the producer's metadata at startup, which fails if it queries before propagation completes. Wait until the
   * topic is fully describable with a leader for every partition, so topic validation does not flake under load.
   */
  protected void awaitTopicMetadataPropagated(final AdminClient adminClient, final String topicName, final int partitions) {
    for (int i = 0; i < 100; i++) {
      try {
        var description = adminClient.describeTopics(Collections.singleton(topicName)).allTopicNames().get().get(topicName);
        boolean ready = description != null
            && description.partitions().size() == partitions
            && description.partitions().stream().allMatch(p -> p.leader() != null && p.leader().id() >= 0);
        if (ready) {
          return;
        }
      } catch (InterruptedException | ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          throw new RuntimeException(e.getMessage(), e);
        }
      }
      ExceptionUtils.doUnchecked(() -> Thread.sleep(50));
    }
    throw new IllegalStateException("Topic '" + topicName + "' metadata did not propagate in time.");
  }

}
