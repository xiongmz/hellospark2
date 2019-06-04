package cn.xiongmz.hellospark.egstreaming;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
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
 * 滑动slides窗口方式 reduceByKeyAndWindow
 * 即每隔a秒计算当前历史b秒的数据，一般b大于a。这样就会有rdd会多次在batch中参与计算
 * 例如：第10分钟统计第1-第10分钟所有RDD，第11分钟统计第2-第11分钟所有RDD
 * 主要应用场景：最近一小时最热门的文章
 * 任何一个窗口操作都需要指定两个参数：
 * 	 window length（窗口长度） - 窗口的持续时间
 *   sliding interval（滑动间隔） - 执行窗口操作的间隔
 * 这两个参数必须是 source DStream 的 batch interval（批间隔）的倍数（例如5秒的倍数）
 * @author xiongmz
 *
 */
public class WindowBasedTopWord {
	public static void main(String[] args) {
		JavaStreamingContext jssc = null;
		try {
			SparkConf conf = new SparkConf().setMaster("local[3]").setAppName("WindowBasedTopWord").set("spark.default.parallelism", "100");
			conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
			jssc = new JavaStreamingContext(conf, Durations.seconds(5));
			jssc.checkpoint("E:/temp/checkpoint");// 存rdd信息

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
			JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = -6879130809928465477L;

				@Override
				public Integer call(Integer v1, Integer v2) throws Exception {
					return v1 + v2;
				}
			}, new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = -3281933352894631903L;

				@Override
				public Integer call(Integer v1, Integer v2) throws Exception {
					return v1 - v2;
				}
			}, Durations.seconds(60), Durations.seconds(10));
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
