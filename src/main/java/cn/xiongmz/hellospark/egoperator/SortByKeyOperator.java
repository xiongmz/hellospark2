package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 按key进行排序
 * @author JackXiong
 *
 */
public class SortByKeyOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer, String>> scoreList = Arrays.asList(
				new Tuple2<Integer, String>(150, "xuruyun"),
				new Tuple2<Integer, String>(100, "liangyongqi"),
				new Tuple2<Integer, String>(90, "wangfei"));
		JavaPairRDD<Integer, String> rdd = sc.parallelizePairs(scoreList);
		// 参数false 倒序从大到小，不写默认为true 正序从小到大
		JavaPairRDD<Integer, String> result = rdd.sortByKey();
		result.foreach(new VoidFunction<Tuple2<Integer,String>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, String> tuple) throws Exception {
				System.out.println(tuple._1+":"+tuple._2);
			}
			/**
			 * output:
			 *  90:wangfei
				100:liangyongqi
				150:xuruyun
			 */
		});
		sc.close();
	}
}
