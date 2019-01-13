package com.kyle.hbase;

import com.kyle.utils.HbaseUtils;
import com.kyle.utils.MyRadomUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.net.URLEncoder;

public class Query_table {

    public void scan(String tableName) throws Exception{

        Connection conn = HbaseUtils.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            showCell(result);
        }
    }

    public void scanRage(String tableName, String startRow, String endRow) throws Exception{
        Connection conn = HbaseUtils.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        String start = MD5Hash.getMD5AsHex(startRow.getBytes()).substring(0,5) + "-" + startRow;
        String stop = MD5Hash.getMD5AsHex(endRow.getBytes()).substring(0,5) + "-" + endRow + "-"+ Long.MAX_VALUE;
        System.out.println("start: " + start + "   stop: " + stop);
        scan.setStartRow(Bytes.toBytes(start));
        scan.setStopRow(Bytes.toBytes(stop));
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            showCell(result);
        }

    }




    private void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            /*System.out.println("rowKey : " + new String(CellUtil.cloneRow(cell)));
            System.out.println("Timetamp: " + cell.getTimestamp());
            System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("colKey: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value: " + new String(CellUtil.cloneValue(cell)));*/
            System.out.println("rowKey: " + new String(CellUtil.cloneRow(cell)) + "; "
                    + "cf: " + new String(CellUtil.cloneFamily(cell)) +";"
                    + "colKey: " +  new String(CellUtil.cloneQualifier(cell)) + ";"
                    + "value: " + new String(CellUtil.cloneValue(cell)));
        }

    }




    public void testMD5(String str1, String str2) throws Exception{

        while (true){
            String telephone = MyRadomUtils.getTelephone();
            if (str1.equals(telephone)){
                String start = MD5Hash.getMD5AsHex(telephone.getBytes()).substring(0,5) + "-" ;
                String stop = MD5Hash.getMD5AsHex(str1.getBytes()).substring(0,5) + "-" ;
                System.out.println(start);
                System.out.println(stop);
                break;
            }
        }






    }







}
