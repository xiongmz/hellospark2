package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Join：以左侧的RDD的key为标准，值进行笛卡尔操作。右侧有多余的会舍弃右侧的
 * @author JackXiong
 *
 */
public class JoinOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> nameList = Arrays.asList(
				new Tuple2<Integer, String>(1, "xuruyun"),
				new Tuple2<Integer, String>(2, "liangyongqi"),
				new Tuple2<Integer, String>(3, "wangfei"),
				new Tuple2<Integer, String>(3, "annie"));
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 150),
				new Tuple2<Integer, Integer>(2, 100),
				new Tuple2<Integer, Integer>(3, 80),
				new Tuple2<Integer, Integer>(3, 90),
				new Tuple2<Integer, Integer>(4, 99));
		JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
		JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);
		
		JavaPairRDD<Integer,Tuple2<String,Integer>> nameScoreRDD = nameRDD.join(scoreRDD);
		nameScoreRDD.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple) throws Exception {
				System.out.println(tuple._1+"-"+tuple._2._1+"-"+tuple._2._2);
				/**
				 * output:
				 *  1-xuruyun-150
					3-wangfei-80
					3-wangfei-90
					3-annie-80
					3-annie-90
					2-liangyongqi-100
				 */
			}
		});
		sc.close();
	}
}
