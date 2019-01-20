package com.kyle.hive;

import java.sql.*;

public class HiveTest01 {

    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://cdh01:10000";
    //private static String url = "jdbc:hive2://172.19.23.153:11000";
    private static String username = "hive";
    private static String password = "";
    private static Connection conn = null;

    static {
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void queryDistinct() throws SQLException {
        String sql = "select count(distinct(product_key)) from fact_order";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.execute("set mapreduce.job.name=test_hive_jdbc");
        ps.execute("use dim_kylin");
        ResultSet rs = ps.executeQuery();
        int col = rs.getMetaData().getColumnCount();
        System.out.println("=====================================");
    }


}
