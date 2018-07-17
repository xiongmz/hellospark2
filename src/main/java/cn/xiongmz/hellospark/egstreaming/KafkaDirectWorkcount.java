package cn.xiongmz.hellospark.egstreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
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
			
			Map<String, String> kafkaParams = new HashMap<String, String>();
			kafkaParams.put("metadata.broker.list", "nd1:9092,nd2:9092,nd3:9092");
			
			Set<String> topics = new HashSet<>();
			topics.add("hellokafka");
			
			// Direct是每隔一段时间主动从kafka里面取数据
			JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
					jssc,
					String.class,// 键类型。一般都是用这个
					String.class,// 值类型。一般都是用这个
					StringDecoder.class, // 键解析器。一般都是用这个
					StringDecoder.class, // 值解析器。一般都是用这个
					kafkaParams,
					topics);
			
			JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>() {
				private static final long serialVersionUID = -1789151233380960724L;
				@Override
				public Iterator<String> call(Tuple2<String, String> tuple) throws Exception {
					// kafka中的元素是tuple类型键值对，键是偏移量，此处没什么意思，真正的数据在value中，即._2
					return Arrays.asList(tuple._2.split(" ")).iterator();
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
