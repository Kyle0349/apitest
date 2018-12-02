package com.kyle.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PhoenixUtils {

    public static Connection getConn() throws ClassNotFoundException, SQLException {

        Class.forName(ConfigUtils.getProp("phoenix.driver"));
        Connection conn = DriverManager.getConnection(
                ConfigUtils.getProp("phoenix.url"),
                ConfigUtils.getProp("phoenix.user"),
                ConfigUtils.getProp("phoenix.password"));

        return conn ;
    }

}
