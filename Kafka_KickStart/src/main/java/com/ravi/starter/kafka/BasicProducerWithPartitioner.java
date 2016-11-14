package com.ravi.starter.kafka;



import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/*
 *<h1>Producer with a Partitioner for collecting Website Logs</h1>
 *<p>
 * The advantage of the custom partitioning logic is that all the log messages 
 * that are generated for the same client IP address will end up going to the same partition. 
 * Also, the same partition may have batch log messages for different IP addresses.
 * <b>Note </> 
 * Below topic needs to be created for this program to run effectively
 *{@code [root@localhost kafka_2.9.2-0.8.1.1]# C:\ITBox\kafka_2.10-0.10.0.1\bin>windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 2 --partitions 5 --topic website-hits }
 *
 */

public class BasicProducerWithPartitioner {

	 private static Producer<String, String> producer;

	  public BasicProducerWithPartitioner() {
	    Properties props = new Properties();

	    // Set the broker list for requesting metadata to find the lead broker
	    props.put("metadata.broker.list",
	          "localhost:9092");

	    // This specifies the serializer class for keys 
	    props.put("serializer.class", "kafka.serializer.StringEncoder");
	    
	    // Defines the class to be used for determining the partition 
	    // in the topic where the message needs to be sent.
	    props.put("partitioner.class", "com.ravi.starter.kafka.BasicPartitioner");
	    
	    // 1 means the producer receives an acknowledgment once the lead replica 
	    // has received the data. This option provides better durability as the 
	    // client waits until the server acknowledges the request as successful.
	    props.put("request.required.acks", "1");
	    
	    ProducerConfig config = new ProducerConfig(props);
	    producer = new Producer<String, String>(config);
	  }

	  public static void main(String[] args) {
	    int argsCount = args.length;
	    if (argsCount == 0 || argsCount == 1)
	      throw new IllegalArgumentException(
	        "Please provide topic name and Message count as arguments");

	    // Topic name and the message count to be published is passed from the
	    // command line
	    String topic = (String) args[0];
	    String count = (String) args[1];
	    int messageCount = Integer.parseInt(count);
	    
	    System.out.println("Topic Name - " + topic);
	    System.out.println("Message Count - " + messageCount);

	    BasicProducerWithPartitioner simpleProducer = new BasicProducerWithPartitioner();
	    simpleProducer.publishMessage(topic, messageCount);
	  }

	  private void publishMessage(String topic, int messageCount) {
	    Random random = new Random();
	    for (int mCount = 0; mCount < messageCount; mCount++) {
	    
	    String clientIP = "192.168.14." + random.nextInt(255); 
	    String accessTime = new Date().toString();

	    String message = accessTime + ",kafka.apache.org," + clientIP; 
	      System.out.println(message);
	      
	      // Creates a KeyedMessage instance
	      KeyedMessage<String, String> data = 
	        new KeyedMessage<String, String>(topic, clientIP, message);
	      
	      // Publish the message
	      producer.send(data);
	    }
	    // Close producer connection with broker.
	    producer.close();
	  }
	}

