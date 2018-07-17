package cn.xiongmz.hellospark.egstreaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

/**
 * 可以和外部RDD联合计算
 * @author xiongmz
 *
 */
public class TransformOperation {

	public static void main(String[] args) {
		JavaStreamingContext jssc = null;
		try {
			SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TransformOperation");
			jssc = new JavaStreamingContext(conf, Durations.seconds(20));
			// 用户点击网站上的广告，点击之后进行实时计算
			// 由于存在用户刷广告，因此提前准备了一个黑名单机制，只要黑名单中的用户，其点击行为无效
			// 先模拟一个黑名单数据RDD，true代表启用，false代表不启用
			List<Tuple2<String, Boolean>> blacklist = new ArrayList<>();
			blacklist.add(new Tuple2<String, Boolean>("yasaka", true));
			blacklist.add(new Tuple2<String, Boolean>("xuruyun", false));

			final JavaPairRDD<String, Boolean> blacklistRDD = jssc.sparkContext().parallelizePairs(blacklist);

			// 源数据每行包含三项信息：time adId name
			JavaReceiverInputDStream<String> adsClickLogDStream = jssc.socketTextStream("nd1", 8888);
			JavaPairDStream<String, String> adsClickLogPairDStream = adsClickLogDStream.mapToPair(new PairFunction<String, String, String>() {
				private static final long serialVersionUID = 5860032757326767107L;

				@Override
				public Tuple2<String, String> call(String line) throws Exception {
					// 根据姓名来标记行数据
					return new Tuple2<String, String>(line.split(" ")[2], line);
				}
			});
			JavaDStream<String> normalLogs = adsClickLogPairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
				private static final long serialVersionUID = 5465913910013517398L;

				@Override
				public JavaRDD<String> call(JavaPairRDD<String, String> userLogBatchRDD) throws Exception {
					JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = userLogBatchRDD.leftOuterJoin(blacklistRDD);
					JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = joinedRDD
							.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
								private static final long serialVersionUID = -2938046110822092099L;

								@Override
								public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
									if (tuple._2._2.isPresent() && tuple._2._2.get()) {
										return false;
									} else {
										return true;
									}
								}
							});
					JavaRDD<String> validLogRDD = filteredRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
						private static final long serialVersionUID = -8939929033439679655L;

						@Override
						public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
							return tuple._2._1;
						}

					});

					return validLogRDD;
				}
			});

			normalLogs.print();// action操作

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
