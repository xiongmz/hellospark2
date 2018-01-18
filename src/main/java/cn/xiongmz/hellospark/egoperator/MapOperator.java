package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * RDD遍历操作
 * @author JackXiong
 *
 */
public class MapOperator {
	//MapOperator：对RDD中每一个元素进行操作
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);// 数据来源为数组

		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);// 数组转换为RDD
		// map对每个元素操作
		JavaRDD<Integer> results = numberRDD.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 7981231129056943812L;

			@Override
			public Integer call(Integer number) throws Exception {
				return number * 10; //对每个元素乘以10
			}
		});
		results.foreach(new VoidFunction<Integer>() {// 迭代数据内从并打印

			private static final long serialVersionUID = 8273443512811768259L;

			@Override
			public void call(Integer result) throws Exception {
				System.out.println(result);
			}
		});
		sc.close();
	}

}
