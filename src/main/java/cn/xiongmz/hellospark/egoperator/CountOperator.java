package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD元素个数（例如统计行数）
 * @author JackXiong
 *
 */
public class CountOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
		JavaRDD<Integer> numbersRDD = sc.parallelize(numberList);
		long count = numbersRDD.count();
		System.out.println(count);//output:9
		sc.close();
	}
}
