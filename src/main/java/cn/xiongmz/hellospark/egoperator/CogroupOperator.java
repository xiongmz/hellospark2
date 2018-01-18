package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * Cogroup与Join不同（一般工作中Join用的较多一些）
 * @author JackXiong
 *
 */
public class CogroupOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<String, String>> studentList = Arrays.asList(
				new Tuple2<String, String>("1", "xuruyun"),
				new Tuple2<String, String>("2", "wangfei"),
				new Tuple2<String, String>("2", "liangyongqi"),
				new Tuple2<String, String>("3", "lixin"));
		List<Tuple2<String, String>> scoreList = Arrays.asList(
				new Tuple2<String, String>("1", "100"),
				new Tuple2<String, String>("2", "90"),
				new Tuple2<String, String>("3", "80"),
				new Tuple2<String, String>("1", "70"),
				new Tuple2<String, String>("2", "60"),
				new Tuple2<String, String>("3", "50"),
				new Tuple2<String, String>("4", "99"));
		JavaPairRDD<String, String> studentRDD = sc.parallelizePairs(studentList);
		JavaPairRDD<String, String> scoreRDD = sc.parallelizePairs(scoreList);
		
		// Cogroup与Join不同（一般工作中Join用的较多一些）
		// 相当于：2个RDD的distinc key 各自RDD内join上所有value，都放到一个Iterable中
		JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> studentScoreRDD = studentRDD.cogroup(scoreRDD);
		studentScoreRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<Iterable<String>,Iterable<String>>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> tuple) throws Exception {
				System.out.println("studentid:"+tuple._1+",name:"+tuple._2._1+",score:"+tuple._2._2);
				/*
				 * output:
				 * studentid:4,name:[],score:[99]
				 * studentid:2,name:[wangfei, liangyongqi],score:[90, 60]
				 * studentid:3,name:[lixin],score:[80, 50]
				 * studentid:1,name:[xuruyun],score:[100, 70]
				 */
			}
		});
		sc.close();
	}
}
