package com.kyle.spark230.main;

import com.kyle.spark230.core.GetMetaInfoFromRMDB;

public class SyncTableFromMysql {

    public static void main(String[] args) {

        GetMetaInfoFromRMDB getMetaInfoFromRMDB = new GetMetaInfoFromRMDB();
        String metaInfoFromMysql = getMetaInfoFromRMDB.getMetaInfoFromMysql("product");
        System.out.println(metaInfoFromMysql);

    }


}
