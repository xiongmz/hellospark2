package cn.xiongmz.hellospark;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		//		SparkConf conf = new SparkConf().setAppName("WordCount");// spark on yarn模式
		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");// 本机模式
		JavaSparkContext sc = new JavaSparkContext(conf);

		// map端：行变成词
		//		JavaRDD<String> text = sc.textFile("hdfs://mycluster/usr/file/test.txt");
		JavaRDD<String> text = sc.textFile("hellospark2/src/main/resources/1.txt", 3);// 3参数指最小的partition数量，因此读完后的partition会大于等于3
		JavaRDD<String> words = text.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}

		});
		// map端：词统计
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}

		});
		//reduce端：
		JavaPairRDD<String, Integer> result = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer value1, Integer value2) throws Exception {
				return value1 + value2;
			}
		});
		// 键值互换
		JavaPairRDD<Integer, String> temp = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<Integer, String>(tuple._2, tuple._1);
			}

		});
		JavaPairRDD<String, Integer> sorted = temp.sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
				return new Tuple2<String, Integer>(tuple._2, tuple._1);
			}

		});
		// 本机模式打印日志
		result.foreach(new VoidFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("word:" + tuple._1 + ",count:" + tuple._2);
			}
		});
		// 集群模式下打印日志（不能写在里面。不会报错，但是不会打印出来）
		//		List<Tuple2<String, Integer>> result2 = result.collect();
		//		for(Tuple2<String, Integer> tuple :result2){
		//			logger.info("wordxxx:" + tuple._1 + ",count:" + tuple._2);
		//		}
		sc.close();
	}

}
