package cn.xiongmz.hellospark.hbase;

/*
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
*/
/**
 * @author xiongmz
 */
public class HelloSparkOnHbase {
	public static void main(String[] args) {
		/*
		SparkConf sparkConf = null;
		Configuration hbaseConf = null;
		JavaSparkContext sc = null;
		Scan scan = null;
		try {
			sparkConf = new SparkConf().setMaster("local").setAppName("HelloSparkOnHbase");
			sc = new JavaSparkContext(sparkConf);

			hbaseConf = HBaseConfiguration.create();
			scan = new Scan();
			scan.addFamily(Bytes.toBytes("cf"));
			String tableName = "ser_msglog";
			hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName);
			ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
			String ScanToString = Base64.encodeBytes(proto.toByteArray());
			hbaseConf.set(TableInputFormat.SCAN, ScanToString);

			JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
			myRDD.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable,Result>>() {
				
				private static final long serialVersionUID = 2465670099016856278L;

				@Override
				public void call(Tuple2<ImmutableBytesWritable, Result> t) throws Exception {
					System.out.println("---*** "+t._1+"---"+t._2+" ***---");
				}
			});
			System.out.println("---*** "+myRDD.count()+" ***---");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (sc != null) {
				sc.close();
			}
		}
	*/
	}
}
