#!/bin/sh


tableName="tb_user"
connection="jdbc:mysql://centos1:3306/test"
username="root"
password="root"
condition=" 1=1 "
hiveDB="test"
hiveTable="hive_tb_user1"
sqoopTmpDir="/tmp/sqoop/${tableName}"


sqoop import -D mapreduce.job.name=sqoop_inport_table_${tableName} \
--connect ${connection} \
--username ${username} \
--password ${password} \
--query "select * from ${tableName} where ${condition} and \$CONDITIONS" \
--hive-database ${hiveDB} \
--hive-table ${hiveTable} \
--hive-import \
--target-dir ${sqoopTmpDir} \
--delete-target-dir \
--hive-drop-import-delims \
--fields-terminated-by '\001' \
--lines-terminated-by '\n' \
--verbose \
--null-string '\\N' \
--null-non-string '\\N' \
--as-parquetfile \
-m 1