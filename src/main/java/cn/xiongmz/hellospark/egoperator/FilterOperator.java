package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 过滤元素
 * call中为true则保留，为false则舍弃
 * @author JackXiong
 *
 */
public class FilterOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Filter算子是过滤，逻辑里面返回为true则保留，false就过滤掉
		// 通常在Filter之后会接一个coalesce算子（属于优化的内容）
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);// 数据来源为数组
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);// 数组转换为RDD

		JavaRDD<Integer> results = numberRDD.filter(new Function<Integer, Boolean>() {

			private static final long serialVersionUID = 7981231129056943812L;

			@Override
			public Boolean call(Integer number) throws Exception {
				return number % 2 == 0; // 判断2的整数
				
			}
		});
		results.foreach(new VoidFunction<Integer>() {// 迭代数据内从并打印

			private static final long serialVersionUID = 8273443512811768259L;

			@Override
			public void call(Integer result) throws Exception {
				System.out.println(result);
				// 输出2、4
			}
		});
		sc.close();
	}

}
