package cn.xiongmz.hellospark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 二次排序：第一列相同的情况下，对第二列继续排序
 * 实现：多列组合形成额外一个key字段，对这个新key进行排序
 * SortByKey是字典排序
 * @author JackXiong
 *
 */
public class SecondSort {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("GroupTopN");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("src/main/resources/secondSort");
		JavaPairRDD<SecondSortKey, String> rdd = lines.mapToPair(new PairFunction<String, SecondSortKey, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<SecondSortKey, String> call(String line) throws Exception {

				return new Tuple2<SecondSortKey, String>(new SecondSortKey(Integer.valueOf(line.split(" ")[0]), Integer.valueOf(line.split(" ")[2])), line);
			}

		});
		JavaPairRDD<SecondSortKey, String> rddSorted = rdd.sortByKey(false);
		JavaRDD<String> result = rddSorted.map(new Function<Tuple2<SecondSortKey, String>, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<SecondSortKey, String> v1) throws Exception {
				return v1._2;
			}

		});
		result.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String line) throws Exception {
				System.out.println(line);
			}
		});
		sc.close();
	}
}
