package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * 按key（第一个key）分组进行计数
 * @author JackXiong
 *
 */
public class CountByKeyOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		// CountByKey：count groupbykey 注意：有shuffle（源码里看是调了reducebykey的）
		// 没有shuffle的算子效率会比较高
		List<Tuple2<String, String>> scoreList = Arrays.asList(
				new Tuple2<String, String>("70s", "xuruyun"), 
				new Tuple2<String, String>("70s", "wangfei"),
				new Tuple2<String, String>("70s", "wangfei"), 
				new Tuple2<String, String>("80s", "yasaka"), 
				new Tuple2<String, String>("80s", "zhengzongwu"),
				new Tuple2<String, String>("80s", "lixin"));
		JavaPairRDD<String, String> students = sc.parallelizePairs(scoreList);
		// 统计70s和80s分别多少人
		// 就是统计每个key对应的元素个数
		Map<String, Long> counts = students.countByKey();
		for (Map.Entry<String, Long> student : counts.entrySet()) {
			System.out.println(student.getKey() + ":" + student.getValue());
		}
		/**
		 * output
		 *  80s:3
			70s:3
		 */
		sc.close();
	}
}
