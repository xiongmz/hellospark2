package cn.xiongmz.hellospark.egsql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class DataFrameCreate {
	public static void main(String[] args) {
		JavaSparkContext sc = null;
		SparkSession sparkSession = null;
		try {
			SparkConf conf = new SparkConf().setMaster("local").setAppName("DataFrameCreate");
			// 使用KryoSerializer来优化序列化性能
			conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
			conf.registerKryoClasses(new Class[]{Student.class});
			sc = new JavaSparkContext(conf);
			sparkSession = SparkSession.builder().config(conf).getOrCreate();
			//
			Student student = new Student();
			student.setName("Andy");
			student.setScore(32);
			Encoder<Student> studentEncoder = Encoders.bean(Student.class);
			Dataset<Student> javaBeanDS = sparkSession.createDataset(Collections.singletonList(student), studentEncoder);
			javaBeanDS.show();

			//
			Encoder<Integer> integerEncoder = Encoders.INT();
			Dataset<Integer> primitiveDS = sparkSession.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
			Dataset<Integer> transformedDS = primitiveDS.map(
			    (MapFunction<Integer, Integer>) value -> value + 1,
			    integerEncoder);
			List<Integer> result = transformedDS.collectAsList(); // Returns [2, 3, 4]
			for (Integer integer : result) {
				System.out.println(integer);
			}

			// 要求json文件每行必须是完整格式的json字符，不能内部字段嵌套，不能一行内包含2个或多个json串（如果有多个则只能识别第一个）
			Encoder<StudentScore> studentScoreEncoder = Encoders.bean(StudentScore.class);
			String path = "src/main/resources/studentscore.json";
			Dataset<StudentScore> studentScoreDS = sparkSession.read().json(path).as(studentScoreEncoder);
			studentScoreDS.show();
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sc != null) {
				sc.close();
			}
			if (sparkSession != null) {
				sparkSession.stop();
			}
		}
	}
}
