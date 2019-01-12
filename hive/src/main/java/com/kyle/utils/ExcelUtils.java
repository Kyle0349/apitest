package com.kyle.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class ExcelUtils {

    private static final String EXCEL_XLS = "xls";
    private static final String EXCEL_XLSX = "xlsx";


    public static Workbook getWorkbook(File excelFile) throws IOException {
        Workbook wb;
        if (null != excelFile){
            if (excelFile.getName().endsWith(EXCEL_XLS)){
                FileInputStream is = new FileInputStream(excelFile);
                wb = new HSSFWorkbook(is);
                return wb;
            }else if (excelFile.getName().endsWith(EXCEL_XLSX)){
                FileInputStream is = new FileInputStream(excelFile);
                wb = new XSSFWorkbook(is);
                return wb;
            }
        }else {
            wb = new XSSFWorkbook();
            return wb;
        }
        return null;
    }


    public static SXSSFWorkbook getSworkbook(){
        SXSSFWorkbook swb = new SXSSFWorkbook(500);
        return swb;
    }


    public static Sheet createSheet(Workbook wb, String sheetName){
        if (StringUtils.isBlank(sheetName)){
            sheetName = "sheet1";
        }
        Sheet sheet = wb.createSheet(sheetName);
        return sheet;
    }

    public static void write2Excel(String wbPath, Workbook wb) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(wbPath);
        wb.write(fileOutputStream);
        wb.close();
        fileOutputStream.close();

    }

}
