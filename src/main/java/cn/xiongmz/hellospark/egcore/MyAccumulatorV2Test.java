package cn.xiongmz.hellospark.egcore;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.util.LongAccumulator;

/**
 * AccumulatorV2 用于集群环境下的累加器接收变量
 * 仅用于SparkContext，不能用于JavaSparkContext。RDD再转JavaRDD
 * @author JackXiong
 *
 */
public class MyAccumulatorV2Test {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("AccumulatorValues");
		SparkContext sc = new SparkContext(conf);//仅用于SparkContext，不能用于JavaSparkContext

		//LongAccumulator是AccumulatorV2的一个子实现
		final LongAccumulator sum = sc.longAccumulator();
		RDD<String> listRDD = sc.textFile("src/main/resources/integer_array", 1);		
		listRDD.toJavaRDD().foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = -5547880719612358807L;

			@Override
			public void call(String t) throws Exception {
				sum.add(Long.parseLong(t));
			}
		});
		System.out.println(sum.value());// output:10
		
		//自定义的
		final MyAccumulatorV2 sum2 = new MyAccumulatorV2(); 
		sc.register(sum2, "sum2xx");//register方法仅能用于SparkContext
		
		RDD<String> listRDD2 = sc.textFile("src/main/resources/integer_array", 1);
		listRDD2.toJavaRDD().foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = -5547880719612358807L;

			@Override
			public void call(String t) throws Exception {
				sum2.add(Integer.parseInt(t));
			}
		});
		System.out.println(sum2.value());// output:14
		
		// JavaSparkContext 与 SparkContext转换
		List<String> list = Arrays.asList("1", "2", "3", "4");
		// parallelize或者textFile均可以
		JavaRDD<String> javaListRDD = JavaSparkContext.fromSparkContext(sc).parallelize(list);		
		// JavaRDD<String> javaListRDD = JavaSparkContext.fromSparkContext(sc).textFile("src/main/resources/integer_array");
		javaListRDD.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = -475679958189175091L;

			@Override
			public void call(String t) throws Exception {
				sum2.add(Integer.parseInt(t));
			}
		});
		System.out.println(sum2.value());// output:28
		sc.stop();
	}
}
