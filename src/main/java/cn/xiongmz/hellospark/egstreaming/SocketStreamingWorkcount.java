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
 * SparkStreaming版的文本计数demo
 * 监听socket
 * @author xiongmz
 *
 */
public class SocketStreamingWorkcount {
	/*
	 * 运行演示说明：
	 * 首先使用nc工具在远程机器上模拟socket输入
	 * yum install -y nc
	 * nc -lk 8888
	 * 然后java main可以在本地run as application启动，也可以在远程spark-submit启动
	 * 远程机器上执行nc -lk 8888后，命令处于前端运行状态，输入一行数据后再输入一行数据，就是在模拟socket数据
	 */
	public static void main(String[] args) {
		JavaStreamingContext jssc = null;
		try {
			//[2]，线程数必须大于等于2，因为ReceiverInputDStream会占用一个进程。如果设置为1，那么只会有1个线程从socket读取数据，没有多余线程来处理数据了。
			SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SocketStreamingWorkcount");
			/*
			 * 创建该对象类似于SparkCore中的JavaSparkContext，类似于SparkSQL中的SQLContext
			 * 该对象除了接受SparkConf参数，还接收一个BatchInterval时间间隔参数，就是说，每收集多长时间的数据划分为一个Batch即RDD去执行
			 * 这里Durations可以设置分钟、秒、毫秒
			 */
			jssc = new JavaStreamingContext(conf, Durations.seconds(5));
			/*
			 * 首先创建输入Dstream，代表一个数据源比如这里从socket或kafka来持续不断的进入实时数据流
			 * 创建一个监听socket数据量，RDD里面的每一个元素就是一行行的文本
			 */
			// from socket
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
			JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
				private static final long serialVersionUID = -903037400730733949L;
				@Override
				public Integer call(Integer n1, Integer n2) throws Exception {
					return n1+n2;
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
