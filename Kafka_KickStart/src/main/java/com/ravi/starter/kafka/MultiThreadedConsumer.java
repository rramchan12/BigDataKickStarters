package com.ravi.starter.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/*
 * <h1>Multi Threaded Consumer </h1>
 * <p>
 * Will work hand in hand with the partitioned Producer and listen to the website_hits topic
 * 
 * {@code Put these arguments to handshake with Partitioned Producer localhost:2181 testgroup website-hits 3}
 */

public class MultiThreadedConsumer {

	private ExecutorService executor;
	private final ConsumerConnector consumer;
	private final String topic;

	public MultiThreadedConsumer(String zookeeper, String groupId, String topic) {
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeper, groupId));
		this.topic = topic;
	}

	private static ConsumerConfig createConsumerConfig(String zookeeper, String groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);

	}

	public void shutdown() {
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public void testMultiThreadConsumer(int threadCount) {

		Map<String, Integer> topicMap = new HashMap<String, Integer>();

		// Define thread count for each topic
		topicMap.put(topic, new Integer(threadCount));

		// Here we have used a single topic but we can also add
		// multiple topics to topicCount MAP
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamsMap = consumer.createMessageStreams(topicMap);

		System.out.println("Topic is : "+topic);
		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamsMap.get(topic);

		// Launching the thread pool
		executor = Executors.newFixedThreadPool(threadCount);

		// Creating an object messages consumption
		int count = 0;
		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			final int threadNumber = count;
			executor.submit(new Runnable() {
				public void run() {
					ConsumerIterator<byte[], byte[]> consumerIte = stream.iterator();
					while (consumerIte.hasNext())
						System.out.println(
								"Thread Number " + threadNumber + ": " + new String(consumerIte.next().message()));
					System.out.println("Shutting down Thread Number: " + threadNumber);
				}
			});
			count++;
		}
		if (consumer != null)
			consumer.shutdown();
		if (executor != null)
			executor.shutdown();
	}

	public static void main(String[] args) {

		String zooKeeper = args[0];
		String groupId = args[1];
		String topic = args[2];
		int threadCount = Integer.parseInt(args[3]);
		MultiThreadedConsumer multiConsumer = new MultiThreadedConsumer(zooKeeper, groupId, topic);
		multiConsumer.testMultiThreadConsumer(threadCount);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException ie) {

		}
		multiConsumer.shutdown();

	}
}
