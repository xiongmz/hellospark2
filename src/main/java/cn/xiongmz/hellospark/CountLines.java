package cn.xiongmz.hellospark;

import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author xiongmz
 */
public class CountLines {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("CountLines");
		JavaSparkContext sc = new JavaSparkContext(conf);
		Long startTime = System.currentTimeMillis();
		// map端：行变成词。3参数指最小的partition数量，因此读完后的partition会大于等于3
		JavaRDD<String> text = sc.textFile("src/main/resources/groupTopN", 3);
		Long count = text.count();
		Long endTime = System.currentTimeMillis();
		System.out.println("行数：" + count + ",耗时：" + (endTime - startTime) / 1000D);
		sc.close();
	}
}
