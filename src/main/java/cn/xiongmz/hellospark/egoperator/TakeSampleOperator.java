package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 随机取n个元素
 * @author JackXiong
 *
 */
public class TakeSampleOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numberList);

		// TakeSample：Take+Sample 先sample采样再take
		// 第二个参数：在sample里面是比率，这里是固定取多少个
		List<Integer> result = numberRDD.takeSample(false, 2);
		for (Integer n : result) {
			System.out.println(n);
		}
		/**
		 * output:
		 *  3
			4
		 */
		sc.close();
	}
}
