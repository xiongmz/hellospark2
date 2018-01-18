package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * 一行多个字符变成多行每行一个字符
 * @author JackXiong
 *
 */
public class FlatMapOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<String> lineList = Arrays.asList("hello xuruyun", "hello xuruyun", "hello wangfei");
		JavaRDD<String> lines = sc.parallelize(lineList);// partion个数未指定则默认取setMaster中local后接的参数值，例如local[2]，local后接参数默认为1
		// FlatMap=Flat+Map（先map 压扁 再flat）
		// 以下例子是根据空格分隔得到每个词
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" ")).iterator();
			}

		});
		words.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String result) throws Exception {
				System.out.println(result);
			}

		});
		/**
		 * output:
		    hello
			xuruyun
			hello
			xuruyun
			hello
			wangfei
		 */
		sc.close();
	}
}
