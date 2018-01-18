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
 * 笛卡尔积
 * @author JackXiong
 *
 */
public class CartesianOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Cartesian：取笛卡尔积
		List<String> clothes = Arrays.asList("T恤", "夹克", "大衣");
		List<String> trousers = Arrays.asList("西裤", "铅笔裤", "牛仔裤");
		JavaRDD<String> clotheRDD = sc.parallelize(clothes);
		JavaRDD<String> trouserRDD = sc.parallelize(trousers);
		JavaPairRDD<String, String> pairs = clotheRDD.cartesian(trouserRDD);
		
		pairs.foreach(new VoidFunction<Tuple2<String,String>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, String> tuple) throws Exception {
				System.out.println(tuple._1 + ":" + tuple._2);
				/* output:
				T恤:西裤
				T恤:铅笔裤
				T恤:牛仔裤
				夹克:西裤
				夹克:铅笔裤
				夹克:牛仔裤
				大衣:西裤
				大衣:铅笔裤
				大衣:牛仔裤
				*/
			}
		});
		// 或者使用collect方式
//		for (Tuple2<String, String> tuple : pairs.collect()) {
//			System.out.println(tuple._1 + ":" + tuple._2);
//		}
		sc.close();
	}
}
