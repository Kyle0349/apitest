package com.kyle.spark230.sparksql;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SparkPartitioner {


    public void sparkPartitioner01(){

        SparkSession sparkSession = SparkUtils.getSparkSession();
        sparkSession.udf().register("substring", new UDF1<String, String>() {
            @Override
            public String call(String str) throws Exception {
                return str.substring(0, str.length()-1);
            };
        }, DataTypes.StringType);

        JavaRDD<String> parallelize = new JavaSparkContext(sparkSession.sparkContext()).parallelize(Arrays.asList(
                "Warsaw1, 2016, 100",
                "Warsaw2, 2017, 200",
                "Warsaw3, 2016, 100",
                "Warsaw4, 2017, 200",
                "Beijing1, 2017, 200",
                "Beijing2, 2017, 200",
                "Warsaw4, 2017, 200",
                "Boston1, 2015, 50",
                "Boston2, 2016, 150"
        ));
        JavaRDD<SparkUDF_Bean01> map = parallelize.map(line -> {
            String[] split = line.split(", ");
            SparkUDF_Bean01 sparkUDF_bean01 = new SparkUDF_Bean01();
            sparkUDF_bean01.setCity(split[0]);
            sparkUDF_bean01.setYear(split[1]);
            sparkUDF_bean01.setAmount(split[2]);
            return sparkUDF_bean01;
        });
        Dataset<Row> dataFrame = sparkSession.createDataFrame(map, SparkUDF_Bean01.class);
        Dataset<Row> select = dataFrame.select("city", "year", "amount");
        List<Object> cities = dataFrame.select("city").toJavaRDD()
                .map(row -> {
                    Object o = row.get(0);
                    Object res = o.toString().substring(0, o.toString().length() - 1);
                    return res;
                }).distinct().collect();
        JavaRDD<Row> rowJavaRDD = select.toJavaRDD();

        JavaPairRDD<String, Row> pairRDD = rowJavaRDD.mapToPair(row -> {
            String city = row.getString(0);
            return new Tuple2<>(city.substring(0, city.length()-1), row);
        });

        JavaPairRDD<String, Row> stringRowJavaPairRDD = pairRDD.partitionBy(new SparkPartition_byName(cities));

        stringRowJavaPairRDD.foreachPartition( fp -> {
            System.out.println("---------------------> Partition start ");
            System.out.println("partitionID is "+ TaskContext.getPartitionId());
            while (fp.hasNext()){
                Tuple2<String, Row> next = fp.next();
                System.out.println(next._1 + ":" + next._2);
            }
            System.out.println("=====================> Partition stop ");
        });


    }


}
