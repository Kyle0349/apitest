package com.kyle.phoenix;

import com.kyle.utils.PhoenixUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PhoenixTest01 {

    public void select(String sql) throws SQLException, ClassNotFoundException {

        Connection conn = PhoenixUtils.getConn();
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery(sql);
        while (resultSet.next()){
            String string = resultSet.getString(0);
            System.out.println(string);
        }

    }


}
