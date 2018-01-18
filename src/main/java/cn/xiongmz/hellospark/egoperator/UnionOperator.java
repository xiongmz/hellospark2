package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 合并（不去重）
 * @author JackXiong
 *
 */
public class UnionOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Union：合并，不产生shuffle操作。比如原本每个RDD均有2个partition，union后会有4个partition
		// 没有去重
		List<String> names = Arrays.asList("xuruyun", "liangyongqi", "wangfei", "yasaka");
		List<String> names1 = Arrays.asList("xuruyun", "liangyongqi2", "wangfei3", "yasaka4");
		JavaRDD<String> nameRDD = sc.parallelize(names, 2);
		JavaRDD<String> nameRDD1 = sc.parallelize(names1, 2);
		nameRDD.union(nameRDD1).foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String name) throws Exception {
				System.out.println(name);

			}
			/**
			 * output:
			 *  xuruyun
				liangyongqi
				wangfei
				yasaka
				xuruyun
				liangyongqi2
				wangfei3
				yasaka4

			 */
		});
		sc.close();
	}
}
