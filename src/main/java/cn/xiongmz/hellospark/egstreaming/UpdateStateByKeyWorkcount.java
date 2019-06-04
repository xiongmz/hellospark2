package cn.xiongmz.hellospark.egstreaming;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 当前RDD的计算逻辑可以和之前RDD数据结果合并（如何合并可以自定义）
 * 代码参考：SocketStreamingWorkcount.java
 * @author xiongmz
 *
 */
public class UpdateStateByKeyWorkcount {
	
	public static void main(String[] args) {
		JavaStreamingContext jssc = null;
		try {
			SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("UpdateStateByKeyWorkcount");
			conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
			jssc = new JavaStreamingContext(conf, Durations.seconds(5));
			
			jssc.checkpoint("E:/temp/checkpoint");//用于记住state。updateStateByKey必须的，固定这样写即可
			
			JavaReceiverInputDStream<String> lines = jssc.socketTextStream("vm-nd1", 8888);
			
			JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
				private static final long serialVersionUID = 5986920588831791764L;
				@Override
				public Iterator<String> call(String line) throws Exception {
					return Arrays.asList(line.split(" ")).iterator();
				}
			});
			JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
				private static final long serialVersionUID = 4819478427121057001L;
				@Override
				public Tuple2<String, Integer> call(String word) throws Exception {
					return new Tuple2<String, Integer>(word, 1);
				}
			});
			//此处不是reduceByKey，而是updateStateByKey
			JavaPairDStream<String, Integer> wordcounts = pairs.updateStateByKey(
					// List<Integer> 是上一次mapToPair后得到的结果
					// 第1个Optional<Integer>为some、none
					// 第2个Optional<Integer>为返回参数 
					new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
						private static final long serialVersionUID = -2471836117779153716L;
						/*
						 * 对于每个单词，每次batch计算的时候都会调用这个函数。
						 * 第一个参数values为该batch中这个key对应的新的一组值，可能有多个。例如2个1（key1，1）（key1，1），那么values就是（1，1）
						 * 第二个参数表示的是这个key之前的状态
						 */
						@Override
						public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
							// Optional有2个子类：some子类、none子类
							// Key有可能之前从来没出现过，意味着之前从类没有更新过状态
							Integer newValue = 0;
							if(state.isPresent()) {
								// 如果这个state存在
								newValue = state.get();
							}
							for(Integer value : values) {
								newValue = newValue + value;
							}
							return Optional.of(newValue);
						}
			});
			wordcounts.print();//action操作
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
