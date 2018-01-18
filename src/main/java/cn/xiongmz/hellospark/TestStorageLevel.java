package cn.xiongmz.hellospark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestStorageLevel {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("TestStorageLevel");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// map端：行变成词
		JavaRDD<String> text = sc.textFile("E:/work/workspace/catalina.out.72W271M-1.txt");// textFile 是延迟执行
		text = text.cache(); // 进行持久化。低版本如果不用变量来接收会导致该行代码不执行，高版本解决了这个问题。为了保险起见，建议还是用变量接一下。不要仅仅写一个text.cache();
		/*
		 *  
		 * 这个270多M的文件用UE打开需要20秒左右，UE显示的行数是720280
		 * 没做持久化时：（第二次有耗时，同时第一次比第二次多是因为第一次有基础初始化的操作造成第一次耗时比较长一点，第二次操作耗时是真正的job耗时）
		 * 第1次:720280,cost:3474
		 * 第2次:720280,cost:1294
		 * 第3次:720280,cost:1052
		 * 
		 * 做持久化之后（持久化后的第一次耗时4686比没有持久化时的第一次时间多是因为多了持久化的时间）
		 * 第1次:720280,cost:4686
		 * 第2次:720280,cost:71
		 * 第3次:720280,cost:47
		 */
		Long starttime = System.currentTimeMillis();
		Long count = text.count();// count 是立即执行
		Long endtime = System.currentTimeMillis();
		System.out.println("第1次:"+count+",cost:"+(endtime-starttime));
		
		Long starttime2 = System.currentTimeMillis();
		Long count2 = text.count();// count 是立即执行
		Long endtime2 = System.currentTimeMillis();
		System.out.println("第2次:"+count2+",cost:"+(endtime2-starttime2));
		
		
		Long starttime3 = System.currentTimeMillis();
		Long count3 = text.count();// count 是立即执行
		Long endtime3 = System.currentTimeMillis();
		System.out.println("第3次:"+count3+",cost:"+(endtime3-starttime3));

		sc.close();
	}

}
