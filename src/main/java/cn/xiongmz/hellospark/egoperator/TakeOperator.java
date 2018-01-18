package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 从RDD中取前几个元素，以list返回
 * @author JackXiong
 *
 */
public class TakeOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numberList);

		// Take：从RDD中取前几个元素，以list返回
		List<Integer> result = numberRDD.take(3);
		for (Integer n : result) {
			System.out.println(n);
		}
		/**
		 * output:
		    1
			2
			3
		 */
		sc.close();
	}
}
