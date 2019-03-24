package com.kyle.spark230.sparksql;

import com.kyle.spark230.utils.SparkUtils;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Spark SQL用UDF实现按列特征重分区
 * 如果有一列name，zz1,zz2,zz3,nn1,nn2,nn3 怎么让这些数据zz的写在一个分区，nn的写在一个分区
 */
public class SparkUDF  implements Serializable {


    public void sparkUDF01(){

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

        //SQL 对某列重分区
        dataFrame.registerTempTable("test");
        Dataset<Row> sql = sparkSession.sql("select substring(city),sum(amount) from test group by substring(city)");
        sql.toJavaRDD().foreachPartition( fp -> {

            System.out.println("---------------------> Partition start ");
            System.out.println("partitionID is "+ TaskContext.getPartitionId());
            while (fp.hasNext()){
                Row next = fp.next();
                System.out.println(next.getString(0) +": " + next.getDouble(1));
            }
            System.out.println("=====================> Partition stop ");
        });

    }

}
