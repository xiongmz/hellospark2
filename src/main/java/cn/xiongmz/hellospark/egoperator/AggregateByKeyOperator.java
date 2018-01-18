package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * 类似于ReduceByKey，可做wordcount（按key进行计数）
 * @author JackXiong
 *
 */
public class AggregateByKeyOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[5]").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("src/main/resources/test.txt");
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1251357151465167199L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}

		});
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});
		// AggregateByKey其实和ReduceByKey差不多，ReduceByKey是AggregateByKey的简化版
		// 当map端和reduce端的逻辑不一样的时候可以使用AggregateByKey
		// 当map端和reduce端的逻辑一样的时候可以使用ReduceByKey
		// AggregateByKey三个参数
		// 第一个参数：每个key的初始值
		// 第二个参数：如何进行Shuffle map-side的本地聚合
		// 第三个参数：如何进行Shuffle reduce-side的全局聚合
		JavaPairRDD<String, Integer> wordCounts = pairs.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}

		}, new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}

		});
		List<Tuple2<String, Integer>> list = wordCounts.collect();
		for (Tuple2<String, Integer> wc : list) {
			System.out.println(wc._1+":"+wc._2);
		}
		/**
		 * output:
		 *  chengdu:1
			love:3
			mianyang:2
			wuhan:1
			i:3
			hubei:1
			china:1
			sichuan:1
		 */
		sc.close();
	}
}
