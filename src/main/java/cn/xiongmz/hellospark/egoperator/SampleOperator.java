package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 随机采样
 * @author JackXiong
 *
 */
public class SampleOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> nameList = Arrays.asList(
				"xuruyun", "liangyongqi", "wangfei0", "yasaka0", "xuruyun", "liangyongqi", "wangfei1", "yasaka1");
		JavaRDD<String> nameRDD = sc.parallelize(nameList, 2);
		// SampleOperator 随机采样
		// 参数一：指一个元素是否可以采用多次，false为不能多次采样
		// 0.33即采样比例，是指从RDD中取30%的数据出来，这个比例是个大概值，不代表精确的占比
		// 第三个参数seed一般不用，仅一般作单元测试时固定输出结果时使用
		nameRDD.sample(false, 0.33).foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String name) throws Exception {
				System.out.println(name);
			}
		});

		sc.close();
	}
}
