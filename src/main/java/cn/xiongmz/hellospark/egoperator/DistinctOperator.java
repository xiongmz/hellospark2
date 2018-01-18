package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 去重
 * @author JackXiong
 *
 */
public class DistinctOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Dinstinct对元素内容进行去重，有shuffle动作
		List<String> names = Arrays.asList("xuruyun", "liangyongqi", "wangfei", "xuruyun");
		JavaRDD<String> nameRDD = sc.parallelize(names, 2);

		nameRDD.distinct().foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String name) throws Exception {
				System.out.println(name);
			}
			/**
			 * output:
			 * xuruyun
			 * liangyongqi
		     * wangfei
			 */
		});
		
		// Dinstinct对元素内容进行去重，有shuffle动作
		List<Tuple2<String, String>> scoreList = Arrays.asList(
				new Tuple2<String, String>("70s", "xuruyun"), 
				new Tuple2<String, String>("70s", "wangfei"),
				new Tuple2<String, String>("70s", "wangfei"), 
				new Tuple2<String, String>("80s", "yasaka"), 
				new Tuple2<String, String>("80s", "yasaka"),
				new Tuple2<String, String>("80s", "lixin"));
		JavaPairRDD<String, String> score = sc.parallelizePairs(scoreList);

		score.distinct().foreach(new VoidFunction<Tuple2<String,String>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, String> t) throws Exception {
				System.out.println(t._1+" "+t._2);
				
			}
			/**
			 * output:
				70s wangfei
				70s xuruyun
				80s yasaka
				80s lixin
			 */
		});
		sc.close();
	}
}
