package com.kyle.impala;

import java.sql.*;

public class ImpalaTest {

    private static String driver = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://172.19.23.153:21050/;auth=noSasl";
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

    public static void main(String[] args) throws Exception{
        ImpalaTest impalaTest = new ImpalaTest();
        impalaTest.select();
    }


    public void descTable() throws SQLException {

        String sql = "DESCRIBE test.product";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        int col = rs.getMetaData().getColumnCount();
        System.out.println("=====================================");
        while (rs.next()) {
            for (int i = 1; i <= col; i++) {
                System.out.print(rs.getString(i) + "\t");
            }
            System.out.print("\n");
        }
        rs.close();
        ps.close();
        System.out.println("=====================================");
    }


    public void createTable() throws SQLException {
        String sql = "create table test07(name string, id string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.execute();
        ps.close();
    }

    public void select() throws SQLException {
        String sql = "select * from test01";
        PreparedStatement ps = conn.prepareStatement(sql);
        //ps.execute("INVALIDATE METADATA default.test01");
        //ps.execute("REFRESH default.test06");
        ResultSet rs = ps.executeQuery();
        int col = rs.getMetaData().getColumnCount();
        System.out.println("=====================================");
        while (rs.next()) {
            for (int i = 1; i <= col; i++) {
                System.out.print(rs.getString(i) + "\t");
            }
            System.out.print("\n");
        }
        rs.close();
        ps.close();
        System.out.println("=====================================");
    }


    public void queryDistinct() throws SQLException {
        String sql = "select count(distinct(name)) from test06";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.execute("REFRESH test06");
        ResultSet rs = ps.executeQuery();
        int col = rs.getMetaData().getColumnCount();
        System.out.println("=====================================");
        while (rs.next()) {
            for (int i = 1; i <= col; i++) {
                System.out.print(rs.getString(i) + "\t");
            }
            System.out.print("\n");
        }
        rs.close();
        ps.close();
        System.out.println("=====================================");
    }


    public void showTables() throws SQLException {
        String sql = "show tables";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        int col = rs.getMetaData().getColumnCount();
        System.out.println("=====================================");
        while (rs.next()) {
            for (int i = 1; i <= col; i++) {
                System.out.print(rs.getString(i) + "\t");
            }
            System.out.print("\n");
        }
        rs.close();
        ps.close();
        System.out.println("=====================================");

    }




    public void testNullVal() throws SQLException {
        String sql = "select * from test.sqoop_test_product where id = 1004;";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        int col = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            for (int i = 1; i <= col; i++) {
                System.out.print(rs.getString(i) + "\t");
            }
            System.out.print("\n");
        }
        rs.close();
        ps.close();
    }


}
