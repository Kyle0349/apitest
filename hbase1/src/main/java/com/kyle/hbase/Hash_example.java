package com.kyle.hbase;

import com.kyle.utils.HbaseUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.io.IOException;
import java.util.*;

public class Hash_example {

    private byte[][] splitKeys;

    public byte[][] calcSplitKeys(int prepareRegions, int baseRecord) {
        int splitKeysNumber = prepareRegions -1;
        splitKeys = new byte[splitKeysNumber][]; // new byte[9][]
        // 使用treeset保存抽样数据，已排序过
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        // 把生成的散列byte[] 添加到rows
        for (int i = 0; i < baseRecord; i++) {
            rows.add(UUID.randomUUID().toString().replaceAll("-","").getBytes());
        }
        int pointer = 0;
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int index = 0;
        int splitKeysBase = baseRecord / prepareRegions;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            if ((pointer != 0) && (pointer % splitKeysBase == 0)) {
                if (index < splitKeysNumber) {
                    splitKeys[index] = tempRow;
                    index++;
                }
            }
            pointer++;
        }
        /*while (rowKeyIter.hasNext()){
            byte[] tmpRow = rowKeyIter.next();
            splitKeys[index] = tmpRow;
            index++;
        }*/
        rows.clear();
        return splitKeys;
    }


    public void createPrdPartionTable(String tbname, String[] colFamilys) throws Exception {
        byte[][] splitKeys = calcSplitKeys(3, 1000000);

        Connection conn = HbaseUtils.getConn();
        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf(tbname);

        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
            } catch (Exception e) {
            }
            admin.deleteTable(tableName);
        }
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for (String col : colFamilys) {
            HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes(col));
            columnDesc.setMaxVersions(1);
            hTableDescriptor.addFamily(columnDesc);

        }
        admin.createTable(hTableDescriptor, splitKeys);
        admin.close();
    }


    public void put_hash(String tableName) throws IOException {
        Connection conn = HbaseUtils.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));

        long currentTime = System.currentTimeMillis();
        Random random = new Random();

        // 模拟put 10000条数据
        for (int i = 10000; i < 100000; i++) {
            currentTime += random.nextInt(1000); byte[] lowT =
                    Bytes.copy(Bytes.toBytes(currentTime), 4, 4); byte[] lowI =
                    Bytes.copy(Bytes.toBytes((long) i), 4, 4); byte[] rowkey =
                    Bytes.add(MD5Hash .getMD5AsHex(Bytes.add(lowI, lowT)).substring(0, 8)
                            .getBytes(), Bytes.toBytes(i));
            Put put = new Put(rowkey);
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"),
                    Bytes.toBytes("make"));
            table.put(put);
        }
        table.close(); System.err.println("数据插入成功");
    }


    public void put_hash_batch(String tableName) throws IOException {
        Connection conn = HbaseUtils.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Put> puts = new ArrayList<>();
        // 模拟put 10000条数据
        for (int i = 0; i < 10000; i++) {
            byte[] rowkey = Bytes.add(MD5Hash .getMD5AsHex(String.valueOf(System.currentTimeMillis()).getBytes()).substring(0, 8)
                            .getBytes(), Bytes.toBytes(i));
            Put put = new Put(rowkey);
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"),
                    Bytes.toBytes(UUID.randomUUID().toString().replaceAll("-","")));
            put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("desc"),
                    Bytes.toBytes(UUID.randomUUID().toString().replaceAll("-","")));
            puts.add(put);
            if (i % 10000 == 0){
                table.put(puts);
                puts.clear();
            }
        }
        if (puts.size() > 0){
            table.put(puts);
        }
        table.close(); System.err.println("数据插入成功");
    }


    public void testBytes(){
        long currentTime = System.currentTimeMillis();
        byte[] bytes = Bytes.toBytes(currentTime);
        for (byte aByte : bytes) {
            System.out.println(aByte);
        }
        byte[] lowT = Bytes.copy(bytes, 4, 4);
        byte[] lowI = Bytes.copy(Bytes.toBytes((long) 1000), 4, 4);
        for (byte b : lowT) {
            System.out.println(b);
        }
        System.out.println("====");

        for (byte b : lowI) {
            System.out.println(b);
        }
    }










}
