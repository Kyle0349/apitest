package com.kyle.impala;

import com.kyle.utils.ExcelUtils;
import org.apache.poi.xssf.streaming.SXSSFCell;
import org.apache.poi.xssf.streaming.SXSSFRow;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ExcelHandler {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://centos1:10000/test";
    private static String user = "hadoop";
    private static String password = "";

    private static Connection conn = null;
    private static Statement stmt = null;


    public void saveExcelStream(String sql, String sheetName, String excelPath) throws Exception {
        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
        stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery(sql);
        SXSSFWorkbook sworkbook = ExcelUtils.getSworkbook();
        SXSSFSheet sxssfSheet = sworkbook.createSheet(sheetName);
        List<String> columnNameList = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            columnNameList.add(columnName);
        }
        int rowNum = 0;
        while (resultSet.next()){
            SXSSFRow sxssfRow = sxssfSheet.createRow(rowNum);
            int cellNum = 0;
            for (String columnName : columnNameList) {
                SXSSFCell sxssfCell = sxssfRow.createCell(cellNum);
                String columnValue = resultSet.getString(columnName);
                sxssfCell.setCellValue(columnValue);
                cellNum++;
            }
            rowNum++;
            if (rowNum > 1048575){
                throw new Exception("记录超出excel最大值： 1048575");
            }
        }
        ExcelUtils.write2Excel(excelPath, sworkbook);
    }


    public void saveExcel() throws Exception{
        Class.forName(driverName);
        conn = DriverManager.getConnection(url,user,password);
        stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("select * from product");

        XSSFWorkbook wb = (XSSFWorkbook) ExcelUtils.getWorkbook(null);
        XSSFSheet xssfSheet = (XSSFSheet) ExcelUtils.createSheet(wb, "test01");
        List<String> columnNameList = new ArrayList<>();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            columnNameList.add(columnName);
        }

        int rowNum = 0;
        while (resultSet.next()){
            XSSFRow row = xssfSheet.createRow(rowNum);
            int cellNum = 0;
            for (String columnName : columnNameList) {
                XSSFCell cell = row.createCell(cellNum);
                String columnValue = resultSet.getString(columnName);
                cell.setCellValue(columnValue);
                cellNum++;
            }
            rowNum++;
        }

        ExcelUtils.write2Excel("/Users/kyle/Desktop/ddddd.xlsx", wb);

    }

}

