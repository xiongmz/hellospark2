package cn.xiongmz.hellospark.egoperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 一次map处理一个partition中所有数据
 * 默认的map一次只处理一行数据
 * @author JackXiong
 *
 */
public class MapPartitionsOperator {
	// 要理解final使用的原因（算子外面的变量需要定义成final，否则在算子内部不能引用。涉及网络传输，因此不希望算子引用时变量有被改动的可能性）
	public static void main(String[] args) {
		// 以下代码在driver端执行
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> names = Arrays.asList("xurunyuan", "liangyongqi", "wangfei");// 数据来源为数组

		JavaRDD<String> nameRDD = sc.parallelize(names);// 数组转换为RDD

		final Map<String, Integer> scoreMap = new HashMap<>();
		scoreMap.put("xurunyuan", 150);
		scoreMap.put("liangyongqi", 100);
		scoreMap.put("wangfei", 90);

		// 以下代码中的call逻辑在executor端执行
		/*
		 * map算子，一次就处理一个partition中一条数据
		 * mappartitions算子，一次处理一个partition中所有数据
		 * 推荐使用场景：
		 * 如果RDD数据不是特别多，那么采用mappartitions会加快处理速度
		 * 比如100亿条数据，一个partition中有10亿条数据，那么不建议用mappartitions，会内存溢出
		 */
		JavaRDD<Integer> scoreRDD = nameRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
			private static final long serialVersionUID = 8750461126229378459L;

			public Iterator<Integer> call(Iterator<String> iterator) throws Exception {
				List<Integer> list = new ArrayList<>();
				while (iterator.hasNext()) {
					String name = iterator.next();
					Integer score = scoreMap.get(name);
					list.add(score);
				}
				return list.iterator();
			}
		});
		scoreRDD.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 6705927365493327232L;

			@Override
			public void call(Integer score) throws Exception {
				System.out.println(score);
			}
			/**
			 * output:
			 * 150
				100
				90
			 */
		});
		sc.close();
	}

}
