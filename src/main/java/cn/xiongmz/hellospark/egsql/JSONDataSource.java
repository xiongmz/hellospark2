package cn.xiongmz.hellospark.egsql;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class JSONDataSource {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("JSONDataSource");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
		// 姓名-分数
		// read from json
		Dataset<Row> df = sparkSession.read().json("src/main/resources/studentscore.json");
		// read from json
		df.createOrReplaceTempView("student_score");
		df.show();
		Dataset<Row> goodScoreStudentsDS = sparkSession.sql("select name,score from student_score where score >= 81");// 成绩大于80的学生姓名+成绩
		List<String> goodScoreStudents = goodScoreStudentsDS.javaRDD().map(new Function<Row, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Row row) throws Exception {
				return row.getString(0);
			}
		}).collect();
//		for(int i=0;i<goodScoreStudents.size();i++) {
//			System.out.println(goodScoreStudents.get(i));
//		}
		
		// 姓名-年龄
		List<String> studentInfoJSONs = new ArrayList<>();
		studentInfoJSONs.add("{\"name\":\"zhangsan\",\"age\":18}");
		studentInfoJSONs.add("{\"name\":\"lisi\",\"age\":17}");
		studentInfoJSONs.add("{\"name\":\"limeimei\",\"age\":19}");
		JavaRDD<String> studentInfoRDD = sc.parallelize(studentInfoJSONs);
		sparkSession.read().json(studentInfoRDD).createOrReplaceTempView("student_infos");;
		String sql = "select name,age from student_infos where name in(";
		for (int i = 0; i < goodScoreStudents.size(); i++) {
			sql += "'" + goodScoreStudents.get(i) + "'";
			if (i < goodScoreStudents.size() - 1) {
				sql += ",";
			}
		}
		sql += ")";		
		System.out.println(sql);
		Dataset<Row> goodScoreStudentsInfoDS = sparkSession.sql(sql);// 成绩大于80的学生姓名+年龄
		goodScoreStudentsInfoDS.show();
		
		// 存库，整合三个字段
		JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentRDD = goodScoreStudentsInfoDS.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			private static final long serialVersionUID = -6098430522565089673L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String, Integer>(String.valueOf(row.get(0)), Integer.valueOf(String.valueOf(row.get(1))));
			}

		}).join(goodScoreStudentsDS.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {

			private static final long serialVersionUID = -5390539538002136846L;

			@Override
			public Tuple2<String, Integer> call(Row row) throws Exception {
				return new Tuple2<String, Integer>(String.valueOf(row.get(0)), Integer.valueOf(String.valueOf(row.get(1))));
			}

		}));
		JavaRDD<Row> goodStudentsRowRDD = goodStudentRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {

			private static final long serialVersionUID = 433074935873538916L;

			@Override
			public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
				return RowFactory.create(tuple._1, tuple._2._1, tuple._2._2);
			}
		});
		List<StructField> fields = new ArrayList<>();
		fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
		fields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));

		StructType structType = DataTypes.createStructType(fields);
		Dataset<Row> goodStudentDF = sparkSession.createDataFrame(goodStudentsRowRDD, structType);
		//在工程目录下多一个文件夹goodStudentJson，里面有输出文件
		goodStudentDF.write().format("json").mode(SaveMode.Overwrite).save("goodStudentJson");
		
		sparkSession.close();
		sc.close();
	}
}
