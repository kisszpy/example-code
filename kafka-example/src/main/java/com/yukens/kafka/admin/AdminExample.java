package com.yukens.kafka.admin;

import org.apache.kafka.clients.admin.*;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * <p>description</p >
 *
 * @author Trump
 * @version 1.0
 * @date 2021/09/20 12:26
 */
public class AdminExample {
  /**
   * c创建topic
   *
   * @param adminClient
   */
  static void createTopic(AdminClient adminClient, String topicName) {
    NewTopic newTopic = new NewTopic(topicName, 2, (short) 1);
    CreateTopicsResult result = adminClient.createTopics(Arrays.asList(newTopic));
    System.out.println(result);
  }

  /**
   * 查看所有topic的list
   *
   * @param adminClient
   * @throws ExecutionException
   * @throws InterruptedException
   */
  static void listTopic(AdminClient adminClient, boolean showInternal) throws ExecutionException, InterruptedException {

    ListTopicsOptions option = new ListTopicsOptions().listInternal(showInternal);
    ListTopicsResult listTopicsResult = adminClient.listTopics(option);
    System.out.println(listTopicsResult.names().get());

    Map<String, TopicListing> stringTopicListingMap = adminClient.listTopics().namesToListings().get();
    System.out.println(stringTopicListingMap);
  }

  /**
   * 删除topic
   * @param adminClient
   * @param topicName
   * @throws ExecutionException
   * @throws InterruptedException
   */
  static void deleteTopic(AdminClient adminClient, String topicName) throws ExecutionException, InterruptedException {
    DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(topicName));
    System.out.println(deleteTopicsResult.all().get());
  }

  static void descTopic(AdminClient adminClient,String topicName) throws ExecutionException, InterruptedException {
    System.out.println(adminClient.describeTopics(Arrays.asList(topicName)).all().get());
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties properties = new Properties();
    properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // get cofig by properties
    AdminClient adminClient = AdminClient.create(properties);

//  createTopic(adminClient);

    listTopic(adminClient, true);

    // delete topic
    // deleteTopic(adminClient, "helloTopic");
    listTopic(adminClient, true);

    descTopic(adminClient,"order_topic");

  }
}
