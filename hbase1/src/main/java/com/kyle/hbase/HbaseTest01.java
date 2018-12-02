package com.kyle.hbase;

import com.kyle.utils.PhoenixUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HbaseTest01 {

    public void select(String sql) throws SQLException, ClassNotFoundException {

        Connection conn = PhoenixUtils.getConn();
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery(sql);
        while (resultSet.next()){
            String string = resultSet.getString(1);
            String gender = resultSet.getString("gender");
            String name = resultSet.getString("name");
            String chinese = resultSet.getString("chinese");
            String math = resultSet.getString("math");

            System.out.println(gender + " | " + name + " | " + chinese + " | " + math);

        }


    }


}
