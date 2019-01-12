package com.kyle.hbase;

import com.kyle.utils.HbaseUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HbaseTest01 {

    private Connection conn;

    {
        try {
            conn = HbaseUtils.getConn();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void getResultByRowKey(String tableName, String rowKey) throws IOException {
        //Connection conn = HbaseUtils.getConn();
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
        //Connection conn = HbaseUtils.getConn();
        List<Get> gets = prepareGets(rowKeys);
        Table table = conn.getTable(TableName.valueOf(tableName));
        Result[] results = table.get(gets);
        for (Result result : results) {
            showCell(result);
        }
        HbaseUtils.close(null, conn, table);

    }


    public void scan(String tableName) throws Exception{
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            System.out.println("row: " + result.getRow());
            showCell(result);
        }
    }




    /**
     * 创建普通表（没有预分区）
     * @param tableName
     * @param cols
     * @throws IOException
     */
    public void createTable(String tableName, String[] cols) throws IOException {
        //Connection conn = HbaseUtils.getConn();
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
     *  创建分区表（预分区）
     * @param tableName
     * @param columnFamilys
     * @throws Exception
     */
    public void createPrePartTable(String tableName, String[] columnFamilys) throws Exception{
        if (StringUtils.isBlank(tableName) || null == columnFamilys || columnFamilys.length < 1){
            System.out.println("error");
        }
        Admin admin = conn.getAdmin();
        if (!admin.tableExists(TableName.valueOf(tableName))){
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String col : columnFamilys) {
                hTableDescriptor.addFamily(new HColumnDescriptor(col));
            }
            byte[][] splitKeys = getSplitKeys();
            admin.createTable(hTableDescriptor, splitKeys);
        }
    }

    /**
     * hbase table 分区数组
     * @return
     */
    private byte[][] getSplitKeys() {
        String[] keys = new String[] { "10", "20", "30", "40", "50",
                "60", "70", "80", "90" };
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }



    /**
     *
     * @param tableName
     * @param userId
     * @param colFamily
     * @param col
     * @param val
     */
    public void insertRow(String tableName, String userId, String colFamily, String col, String val)
            throws IOException {
        //Connection conn = HbaseUtils.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        String rowKey = userId  +"-"+ String.valueOf(Long.MAX_VALUE - System.currentTimeMillis());
        Put put = preparePut(rowKey, colFamily, col, val);
        table.put(put);
        HbaseUtils.close(null, conn, table);
        HbaseUtils.close(null, conn, table);
    }

    /**
     * 批量插入
     */
    public void insertBatchRow(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.put(hashBatchPut());
    }



    /**
     * 根据rowKey删除行
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public void deleteRow(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        byte[] row = table.get(new Get(Bytes.toBytes(rowKey))).getRow();
        Delete delete = new Delete(row);
        table.delete(delete);
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


    private List<Put> batchPut(){
        ArrayList<Put> list = new ArrayList<>();
        for(int i = 1; i <= 10000; i++){
            String rowKey = String.valueOf(100107000 + i)  +"-"+ String.valueOf(Long.MAX_VALUE - System.currentTimeMillis());
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"),
                    Bytes.toBytes(UUID.randomUUID().toString().replaceAll("-","").substring(0,5)));
            put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("salary"),
                    Bytes.toBytes(new Random(System.currentTimeMillis()).nextInt(100000)));
            list.add(put);
        }
        return list;
    }

    public List<Put> hashBatchPut(){
        ArrayList<Put> list = new ArrayList<>();
        for(int i = 1; i <= 10000; i++){
            String rowKey = MD5Hash.getMD5AsHex(String.valueOf(100107000 + i).getBytes()) + "-" + String.valueOf(100107000 + i).substring(0,5)
                    +"-"+ String.valueOf(Long.MAX_VALUE - System.currentTimeMillis());
            System.out.println("row: " + rowKey);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"),
                    Bytes.toBytes(UUID.randomUUID().toString().replaceAll("-","").substring(0,5)));
            put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("salary"),
                    Bytes.toBytes(new Random(System.currentTimeMillis()).nextInt(100000)));
            list.add(put);
        }
        return list;
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
