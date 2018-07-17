package cn.xiongmz.hellospark.egcore;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Accumulator 用于集群环境下的累加器接收变量
 * @author JackXiong
 *
 */
@SuppressWarnings("deprecation")
public class AccumulatorValue {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("AccumulatorValues");
		JavaSparkContext sc = new JavaSparkContext(conf);

		//已过时，建议选择 Accumulatorv2
		final Accumulator<Integer> sum = sc.accumulator(0, "Our Accumulator ");
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		listRDD.foreach(new VoidFunction<Integer>() {

			private static final long serialVersionUID = 1L;

			// 对1,2,3,4,5进行累加
			@Override
			public void call(Integer t) throws Exception {
				sum.add(t);
			}
		});
		System.out.println(sum.value());// output:15
		
		
		final Accumulator<Double> sum2 = sc.doubleAccumulator(0.1);
		List<Double> list2 = Arrays.asList(9.0,10.0);
		JavaRDD<Double> listRDD2 = sc.parallelize(list2);

		listRDD2.foreach(new VoidFunction<Double>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Double t) throws Exception {
				sum2.add(t);
			}
		});
		System.out.println(sum2.value());// output:19.1
		sc.close();
	}
}
