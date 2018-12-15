package com.kyle.impala;

import java.sql.*;

public class ImpalaTest {

    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        System.out.println("123");

        String driver = "org.apache.hive.jdbc.HiveDriver";
        String url = "jdbc:hive2://centos1:21050/;auth=noSasl";
        String username = "";
        String password = "";
        Connection conn = null;
        Class.forName(driver);
        conn = (Connection) DriverManager.getConnection(url, username, password);


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
        System.out.println("=====================================");
    }
}
