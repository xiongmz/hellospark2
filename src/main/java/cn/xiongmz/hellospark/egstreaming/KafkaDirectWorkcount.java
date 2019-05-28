package cn.xiongmz.hellospark.egstreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * SparkStreaming版的文本计数demo
 * Direct方式：每隔一段时间主动从kafka里面取数据
 * @author xiongmz
 *
 */
public class KafkaDirectWorkcount {
	
	public static void main(String[] args) {
		JavaStreamingContext jssc = null;
		try {
			//[2]，线程数必须大于等于2，因为ReceiverInputDStream会占用一个进程。如果设置为1，那么只会有1个线程从socket读取数据，没有多余线程来处理数据了。
			SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("KafkaDirectWorkcount");
			jssc = new JavaStreamingContext(conf, Durations.seconds(5));

			String brokerList = "nd1:9092,nd2:9092,nd7:9092";
			String topic = "hellokafka";
			Map<String, Object> kafkaParams = new HashMap<String, Object>(16);
			kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
			kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group");
			kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
			Set<String> topics = new HashSet<>();
			topics.add(topic);
			
			// Direct是每隔一段时间主动从kafka里面取数据
			JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
					jssc,
					LocationStrategies.PreferConsistent(),
					ConsumerStrategies.Subscribe(topics, kafkaParams));
			// Get the lines
			JavaDStream<String> lines = messages.map(ConsumerRecord::value);

			JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
				@Override
				public Iterator<String> call(String s) throws Exception {
					return Arrays.asList(s.split(" ")).iterator();
				}
			});
			JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
				private static final long serialVersionUID = 4819478427121057001L;
				@Override
				public Tuple2<String, Integer> call(String word) throws Exception {
					return new Tuple2<String, Integer>(word, 1);
				}
			});
			JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = -903037400730733949L;
				@Override
				public Integer call(Integer n1, Integer n2) throws Exception {
					return n1+n2;
				}
			});
			wordcounts.print();//action操作
			//wordcounts.print();//action操作
			// 固定行
			jssc.start();
			jssc.awaitTermination();
			// 固定行
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (jssc != null) {
				jssc.stop();
			}
		}
	}
}
