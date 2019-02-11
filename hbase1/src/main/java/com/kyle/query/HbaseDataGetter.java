package com.kyle.query;

import com.kyle.utils.HbaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public class HbaseDataGetter implements Callable<List<Result>> {

    private String tableName;
    private List<String> rowKeys;
    private List<String> filterColumn;
    private boolean isContiansRowkeys;
    private boolean isContainsList;
    SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");


    public HbaseDataGetter(String tableName, List<String> rowKeys, List<String> fileterColumn,
                           boolean isContianRowkeys, boolean isContainsList){
        this.tableName = tableName;
        this.rowKeys = rowKeys;
        this.filterColumn = fileterColumn;
        this.isContainsList = isContainsList;
        this.isContiansRowkeys = isContianRowkeys;

    }

    @Override
    public List<Result> call() throws Exception {
        Object[] datasFromHbase = getDatasFromHbase(rowKeys, filterColumn);
        List<Result> listData = new ArrayList<>();
        for (Object o : datasFromHbase) {
            Result r = (Result) o;
            listData.add(r);
        }
        System.out.println("---3: " + sdf.format(System.currentTimeMillis()));
        Thread.sleep(5000);
        System.out.println("---1: " + sdf.format(System.currentTimeMillis()));
        return listData;
    }


    private Object[] getDatasFromHbase(List<String> rowKeys,
                                       List<String> filterColumns) throws IOException {
        Object[] objects = null;
        Connection conn = HbaseUtils.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        List<Get> listGets = new ArrayList<>();
        for (String rk : rowKeys) {
            Get get = new Get(Bytes.toBytes(rk));
//            if (filterColumns != null){
//                for (String fc : filterColumns) {
//                    get.addColumn("".getBytes(),fc.getBytes());
//                }
//            }
            listGets.add(get);
        }
        try {
            objects = table.get(listGets);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            listGets.clear();
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return objects;
    }

}
