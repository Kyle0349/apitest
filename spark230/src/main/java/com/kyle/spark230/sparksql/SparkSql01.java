package com.kyle.spark230.sparksql;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class SparkSql01 implements Serializable {

    public void readFromMysql1(SparkSession sparkSession, String url, String table, Properties properties){
        Dataset<Row> ds = sparkSession.read().format("jdbc").jdbc(url, table, properties);
        ds.show(10);

    }


    public void readFromMysql2(
            SparkSession sparkSession,
            String url,
            String table,
            String column,
            String lowerbound,
            String upperbound,
            int numPartitions) throws InterruptedException {


        Dataset<Row> ds = sparkSession.read().format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("user", "root")
                .option("password", "root")
                .option("numPartitions", numPartitions)
                .option("partitionColumn", column)
                .option("lowerBound", lowerbound)
                .option("upperBound", upperbound).load();

        ds.persist();


        Dataset<Row> item_id_count_ds = ds.groupBy("user_id")
                .agg(count("item_id").alias("item_id_count"));

        item_id_count_ds.show(10);

        item_id_count_ds.agg(sum("item_id_count")).show();

        Thread.sleep(1000*3000);

    }


    public void readFromMysql3(SparkSession sparkSession,
                               String url,
                               String table){

        Dataset<Row> ds = sparkSession.read().format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("user", "root")
                .option("password", "root")
                .load();


        ds.groupBy("user_id").agg(ImmutableMap.of("item_id", "count"))
                .show(10);
    }



    public void writeMysqlRand(SparkSession sparkSession, String url) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        Dataset<Row> dim_product = fetchAllTable(sparkSession, url, "dim_product");
        Dataset<Row> dim_custom = fetchAllTable(sparkSession, url, "dim_custom");
        Dataset<Row> dim_day = fetchAllTable(sparkSession, url, "dim_tday");
        Dataset<Row> dim_salesperson = fetchAllTable(sparkSession, url, "dim_salesperson");

        int num = 0;
        ArrayList<String> rows = new ArrayList<>();
        Row custiom;
        Row product;
        Row tday;
        Row salesperson;
        String custiomString;
        String productString;
        String tdayString;
        String salespersonString;
        String row;
        while (true){
            int quantity = new Random(System.currentTimeMillis()).nextInt(5) + 1;
            double discount = (new Random(System.currentTimeMillis()).nextInt(20) + 80)/100.0;
            Dataset<Row> sample = dim_custom.sample(true, 0.2, System.currentTimeMillis());
            if(!sample.toLocalIterator().hasNext()) continue;
            custiom = sample.first();

            Dataset<Row> sample1 = dim_product.sample(true, 0.2, System.currentTimeMillis());
            if(!sample1.toLocalIterator().hasNext()) continue;
            product = sample1.first();

            Dataset<Row> sample2 = dim_day.sample(true, 0.2, System.currentTimeMillis());
            if(!sample2.toLocalIterator().hasNext()) continue;
            tday = sample2.first();

            Dataset<Row> sample3 = dim_salesperson.sample(true, 0.3, System.currentTimeMillis());
            if(!sample3.toLocalIterator().hasNext()) continue;
            salesperson = sample3.first();

            custiomString = custiom.getString(0);
            productString = product.getString(0);
            double productPrice = product.getDouble(2);
            tdayString = tday.getString(0);
            salespersonString = salesperson.getString(0);
            double orderPay = quantity * productPrice;
            double costPay = orderPay * discount;
            row = String.join(",",tdayString,productString,
                    salespersonString,custiomString,
                    String.valueOf(quantity),String.valueOf(orderPay),String.valueOf(costPay));
            rows.add(row);

            num++;
            if (num > 10) {
                JavaRDD<String> lines = jsc.parallelize(rows);
                JavaRDD<Row> lineRdd = lines.map(new Function<String, Row>() {
                    @Override
                    public Row call(String line) throws Exception {
                        String[] split = line.split(",");
                        return RowFactory.create(split[0], split[1], split[2], split[3],
                                Integer.valueOf(split[4]), Double.valueOf(split[5]),
                                Double.valueOf(split[6]));
                    }
                });
                List<StructField> structFields = new ArrayList<>();
                structFields.add(DataTypes.createStructField("time_key", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("product_key", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("salesperson_key", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("custom_key", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("quantity_ordered", DataTypes.IntegerType, true));
                structFields.add(DataTypes.createStructField("order_pay", DataTypes.DoubleType, true));
                structFields.add(DataTypes.createStructField("cost_pay", DataTypes.DoubleType, true));
                StructType structType = DataTypes.createStructType(structFields);
                Dataset<Row> dataFrame = sparkSession.createDataFrame(lineRdd, structType);
                dataFrame.registerTempTable("data");
                Properties prop = new Properties();
                prop.put("user", "root");
                prop.put("password", "root");
                prop.put("driver","com.mysql.jdbc.Driver");
                dataFrame.write().mode(SaveMode.Append).jdbc(url, "fact_order", prop);
                rows.clear();
            }
            if(num > 10000) break;
        }
    }




    public Dataset<Row> fetchAllTable(SparkSession sparkSession, String url, String table){
        return sparkSession.read().format("jdbc")
                .option("url", url)
                .option("dbtable", table)
                .option("user", "root")
                .option("password", "root")
                .load();
    }


    public void writeMysqlRand2(SparkSession sparkSession, String url) throws SQLException {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        jsc.setLogLevel("WARN");
        Dataset<Row> dim_product = fetchAllTable(sparkSession, url, "dim_product");
        Dataset<Row> dim_custom = fetchAllTable(sparkSession, url, "dim_custom");
        Dataset<Row> dim_day = fetchAllTable(sparkSession, url, "dim_tday");
        Dataset<Row> dim_salesperson = fetchAllTable(sparkSession, url, "dim_salesperson");

        ArrayList<String> rows = new ArrayList<>();
        Row custiom;
        Row product;
        Row tday;
        Row salesperson;
        String custiomString;
        String productString;
        String tdayString;
        String salespersonString;


        String userName = "root";// MySQL默认的root账户名
        String password = "root";// 默认的root账户密码为空
        Connection conn = DriverManager.getConnection(url, userName, password);

        Statement stmt = conn.createStatement();
        int n = 0;
        while (true) {
            int quantity = new Random(System.currentTimeMillis()).nextInt(5) + 1;
            double discount = (new Random(System.currentTimeMillis()).nextInt(20) + 80) / 100.0;
            Dataset<Row> sample = dim_custom.sample(true, 0.2, System.currentTimeMillis());
            if (!sample.toLocalIterator().hasNext()) continue;
            custiom = sample.first();

            Dataset<Row> sample1 = dim_product.sample(true, 0.2, System.currentTimeMillis());
            if (!sample1.toLocalIterator().hasNext()) continue;
            product = sample1.first();

            Dataset<Row> sample2 = dim_day.sample(true, 0.2, System.currentTimeMillis());
            if (!sample2.toLocalIterator().hasNext()) continue;
            tday = sample2.first();

            Dataset<Row> sample3 = dim_salesperson.sample(true, 0.3, System.currentTimeMillis());
            if (!sample3.toLocalIterator().hasNext()) continue;
            salesperson = sample3.first();

            custiomString = custiom.getString(0);
            productString = product.getString(0);
            double productPrice = product.getDouble(2);
            tdayString = tday.getString(0);
            salespersonString = salesperson.getString(0);
            double orderPay = quantity * productPrice;
            double costPay = orderPay * discount;
            String sql = "insert into fact_order values(" +
                    "\""+tdayString+"\"," +
                    "\""+productString+"\"," +
                    "\""+salespersonString+"\"," +
                    "\""+custiomString+"\"," +
                    ""+quantity+"," +
                    ""+orderPay+"," +
                    ""+costPay +
                    ")";
            //System.out.println(sql);
            stmt.execute(sql);
            n++;
            if (n > 10000) break;
        }

    }










}
