package cn.xiongmz.hellospark.egstreaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

/**
 * SparkStreaming版的文本计数demo
 * Receiver方式：长时间监听kafka数据源
 * 在0.10.0 or higher中已经停止了Receiver DStream功能，只保留Direct DStream
 * @author xiongmz
 *
 */
public class KafkaReceiverWorkcount {
	
	public static void main(String[] args) {
		JavaStreamingContext jssc = null;
		try {
			//[2]，线程数必须大于等于2，因为ReceiverInputDStream会占用一个进程。如果设置为1，那么只会有1个线程从socket读取数据，没有多余线程来处理数据了。
			SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("KafkaReceiverWorkcount");
			
			jssc = new JavaStreamingContext(conf, Durations.seconds(5));
			Map<String, Integer> kafkaParams = new HashMap<String, Integer>();
			kafkaParams.put("hellokafka", 1);// 数值1是线程数量。即用几个线程来接收kafka的数据
			String zkList = "nd1:2181,nd2:2181,nd3:2181";
			
			// KafkaUtils是在spark-streaming-kafka依赖中
			/*
			 * Receiver的缺点是会占用一个线程来接数据，如果这个线程所在的机器down了，那么就接收不到数据了
			 * 不同的数据源得要不同的线程
			 * 基于这个缺点及其他原因，有另外的方式来接收数据：Direct
			 */
			JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
					jssc, zkList, "WordcountConsumerGroup", kafkaParams);
			
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
