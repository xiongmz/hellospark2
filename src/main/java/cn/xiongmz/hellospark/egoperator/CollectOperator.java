package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * 集群中的结果拉取到driver端（对于结果数据是大数据量的话慎用）
 * @author JackXiong
 *
 */
public class CollectOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		JavaRDD<Integer> doubleNumbers = numbers.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v) throws Exception {
				return v * 2;
			}
		});
		// 建议用foreach action操作，在远程集群上遍历RDD元素
		// 用Collect操作，将分布式的在远程集群里面的数据拉取到Driver端本地
		// 这种方式不建议使用，如果数据量大的话会产生大量的网络传输甚至Driver端内存不大的话会OOM内存溢出，一般来说都是foreach操作
		List<Integer> doubleNumberList = doubleNumbers.collect();
		for (Integer num : doubleNumberList) {
			System.out.println(num);
		}
		sc.close();
	}
}
