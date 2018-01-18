package cn.xiongmz.hellospark.egoperator;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 按key整合成value迭代
 * @author JackXiong
 *
 */
public class GroupByKeyOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[5]").setAppName(Thread.currentThread().getStackTrace()[1].getClassName());
//		conf.set("spark.default.parallelism", "5");//每个步骤默认的并行度
		JavaSparkContext sc = new JavaSparkContext(conf);
		// GroupByKey：将相同的key的元素放在一起
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("xuruyun", 150), 
				new Tuple2<String, Integer>("liangyongqi", 100),
				new Tuple2<String, Integer>("wangfei", 110), 
				new Tuple2<String, Integer>("wangfei", 80));
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scoreList);
		
		//样例1
//		rdd.groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
//
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
//				System.out.println(tuple._1 + " " + tuple._2);
//			}
//		});
		
		
		// 样例2
		JavaPairRDD<String, Integer> rddMapped = rdd.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<String, Integer>(tuple._1, tuple._2 + 2);
			}
			
		});
		JavaPairRDD<String, Integer> result = rddMapped.repartition(10);
		// repartition成10个后，如果设置了conf.set("spark.default.parallelism", "5")那么groupByKey(不带参)采用的就是5个，否则沿用repartition后的个数10
		// 如果在repartition与groupByKey中间加一个map操作（窄依赖），还是5个task
		JavaPairRDD<String, Iterable<Integer>> finalResult = result.groupByKey();
		finalResult.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
				System.out.println(t._1+" "+ t._2);
				
			}
		});
		sc.close();
		/**
		 *  liangyongqi [102]
			wangfei [82, 112]
			xuruyun [152]
		 */
	}

}
