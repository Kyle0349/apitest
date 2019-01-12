package com.kyle.hbase;

import com.kyle.utils.HbaseUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

public class Query_table {

    public void scan(String tableName) throws Exception{

        Connection conn = HbaseUtils.getConn();
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            showCell(result);
        }
    }

    private void showCell(Result result) {
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("rowKey : " + new String(CellUtil.cloneRow(cell)));
            System.out.println("Timetamp: " + cell.getTimestamp());
            System.out.println("column Family: " + new String(CellUtil.cloneFamily(cell)));
            System.out.println("colKey: " + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("value: " + new String(CellUtil.cloneValue(cell)));
        }

    }
}
