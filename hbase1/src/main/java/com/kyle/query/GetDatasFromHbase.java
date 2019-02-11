package com.kyle.query;

import org.apache.hadoop.hbase.client.Result;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GetDatasFromHbase {


    private ExecutorService pool = Executors.newFixedThreadPool(3);
    private SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

    public List<Result> getDatasFromHbase(final String tableName, final List<String> rowKeys,
                                  final List<String> filterColumn, boolean isContiansRowkeys,
                                  boolean isContainsList){
        if (rowKeys == null || rowKeys.size() <= 0)
        {
            return null;
        }
        final int maxRowKeySize = 1;
        int loopSize = rowKeys.size() % maxRowKeySize == 0 ? rowKeys.size()
                / maxRowKeySize : rowKeys.size() / maxRowKeySize + 1;
        ArrayList<Future<List<Result>>> results = new ArrayList<>();
        for (int loop = 0; loop < loopSize; loop++)
        {
            int end = (loop + 1) * maxRowKeySize > rowKeys.size() ? rowKeys
                    .size() : (loop + 1) * maxRowKeySize;
            List<String> partRowKeys = rowKeys.subList(loop * maxRowKeySize, end);
            HbaseDataGetter hbaseDataGetter = new HbaseDataGetter(tableName,partRowKeys,
                    filterColumn, isContiansRowkeys, isContainsList);
            synchronized (pool)
            {
                Future<List<Result>> result = pool.submit(hbaseDataGetter);
                System.out.println("result: " + result);
                results.add(result);

            }
            System.out.println("---2: " + sdf.format(System.currentTimeMillis()));
        }
        List<Result> dataQueue = new ArrayList<>();
        try
        {
            for (Future<List<Result>> result : results)
            {
                List<Result> rd = result.get();
                dataQueue.addAll(rd);
            }
        }
        catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace();
        }
        return dataQueue;
    }

}
