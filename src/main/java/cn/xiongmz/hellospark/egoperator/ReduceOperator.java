package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * 对元素进行计算汇总
 * @author JackXiong
 *
 */
public class ReduceOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbersRDD = sc.parallelize(numberList);//numbersRDD指针在Driver端，数据本身不一定在Driver端
		// Reduce原理：首先将第1个和第2个元素传入call方法，计算一个结果，然后用这个结果与后一个元素计算，依次类推
		// result作为结果会返回存在Driver端，算子里面的运算是在从节点进行
		// 调reduce方法是在Driver端运行，但是里面的function实在Executor端
		int result = numbersRDD.reduce(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
				/**
				 * output:
				 * 15
				 */
			}
		});
		System.out.println(result);
		sc.close();
	}
}
