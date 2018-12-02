package com.kyle.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PhoenixUtils {

    public static Connection getConn() throws SQLException, ClassNotFoundException {

        Class.forName(ConfigUtils.getProperty("phoenix.driver"));
        Connection conn = DriverManager.getConnection(
                ConfigUtils.getProperty("phoenix.url"),
                ConfigUtils.getProperty("phoenix.user"),
                ConfigUtils.getProperty("phoenix.password"));
        return conn;

    }


}
