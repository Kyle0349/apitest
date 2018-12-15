package com.kyle.hbase;

import com.kyle.utils.HbaseUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseTest01 {

    /**
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void getResultByRowKey(String tableName, String rowKey) throws IOException {
        Connection conn = HbaseUtils.getConn();
        Get get = new Get(Bytes.toBytes(rowKey));
        Table table = conn.getTable(TableName.valueOf(tableName));
        Result result = table.get(get);
        showCell(result);

    }


    /**
     *
     * @param tableName
     * @param rowKeys
     * @throws IOException
     */
    public void getDataByRowKeys(String tableName, List<String> rowKeys) throws IOException {
        Connection conn = HbaseUtils.getConn();
        List<Get> gets = prepareGets(rowKeys);
        Table table = conn.getTable(TableName.valueOf(tableName));
        Result[] results = table.get(gets);
        for (Result result : results) {
            showCell(result);
        }
        HbaseUtils.close(null, conn, table);

    }



    /**
     *
     * @param tableName
     * @param cols
     * @throws IOException
     */
    public void createTable(String tableName, String[] cols) throws IOException {
        Connection conn = HbaseUtils.getConn();
        Admin admin = conn.getAdmin();
        TableName tableName1 = TableName.valueOf(tableName);
        if (!admin.tableExists(tableName1)){
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName1);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }
        HbaseUtils.close(admin, conn);
    }


    /**
     *
     * @param tableName
     * @param rowKey
     * @param colFamily
     * @param col
     * @param val
     */
    public void insertRow(String tableName, String rowKey, String colFamily, String col, String val)
            throws IOException {
        Connection conn = HbaseUtils.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = preparePut(rowKey, colFamily, col, val);
        table.put(put);
        HbaseUtils.close(null, conn, table);
        HbaseUtils.close(null, conn, table);
    }


    /**
     *
     * @param rowKeys
     * @return
     */
    public List<Get> prepareGets(List<String> rowKeys){
        List<Get> gets = null;
        if (rowKeys != null && rowKeys.size() > 0){
            gets = new ArrayList<>();
            for (String rowKey : rowKeys) {
                gets.add(new Get(Bytes.toBytes(rowKey)));
            }
        }
        return gets;
    }






    /**
     *
     * @param rowKey
     * @param colFamily
     * @param col
     * @param val
     * @return
     */
    private Put preparePut(String rowKey, String colFamily, String col, String val){
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        return put;
    }


    private void showCell(Result result){
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("RowName: " + new String(CellUtil.cloneRow(cell)));
            System.out.println("Timetamp: " + cell.getTimestamp());
            System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("row Name: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value: " + new String(CellUtil.cloneValue(cell)));
        }


    }



}
