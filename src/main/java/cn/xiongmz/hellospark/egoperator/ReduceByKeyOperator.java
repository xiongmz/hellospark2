package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 按key进行reduce端（对value进行计算）处理
 * @author JackXiong
 *
 */
public class ReduceByKeyOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[5]").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
		JavaSparkContext sc = new JavaSparkContext(conf);
		// ReduceByKey = GroupByKey + Reduce
		// ReduceByKey在map端自带Combiner
		// 由于自带Combiner，因此进行avage计算时如果数据不在同一个partition，就会有问题。例如1,2在一个,3在另外一个，那么就会1与2平均得到1.5，1.5与3平均就不是2了。这个时候就应该用AggregateByKey
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("xuruyun", 150), 
				new Tuple2<String, Integer>("liangyongqi", 100),
				new Tuple2<String, Integer>("wangfei", 110), 
				new Tuple2<String, Integer>("wangfei", 80));
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scoreList);

		rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {

			private static final long serialVersionUID = 1L;
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).foreach(new VoidFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> tuple) throws Exception {
				System.out.println("姓名:" + tuple._1 + ",得分：" + tuple._2);

			}
			/**
			 * output:
			 *  姓名:xuruyun,得分：150
				姓名:liangyongqi,得分：100
				姓名:wangfei,得分：190
			 */
		});

		sc.close();
	}
}
