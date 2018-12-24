#!/bin/sh

# parameters
queueName='default'
connection='jdbc:mysql://centos1:3306/test'
userName='root'
password='root'
hiveDB='test'
destTablePrefix='sqoop_test'
sqoopTmpDir='/tmp/sqoop/import/test'
fieldsSeparator='\001'
linesSeparator='\n'
keyColumn='id'

endPoint=`date '+%Y-%m-%d %H:%M:%S'`

# srcTables
srcTables='
product
'


#begin to import data into hivedb
for tableName in ${srcTables}
do

# get the start point
startFrom_tmp=`hive -e "
set mapred.job.name=get_start_point_from_hive;
select max(${keyColumn}) from ${hiveDB}.${destTablePrefix}_${tableName};
" > rest.txt`
if [[ $? -ne 0  ]]
then
condition=" 1=1 "
else
startFrom=`head -n 1 rest.txt`
condition=" ${keyColumn} > '${startFrom}' "
fi

sqoop import -D mapred.job.queue.name=${queueName} -D mapred.job.name=sqoop_import_${hiveDB}_${destTablePrefix}_${tableName} \
--connect ${connection} \
--username ${userName} \
--password ${password} \
--query "select * from ${tableName} where ${condition} and \$CONDITIONS " \
--hive-database ${hiveDB} \
--hive-table ${destTablePrefix}_${tableName} \
--hive-import \
--target-dir ${sqoopTmpDir}/${tableName} \
--delete-target-dir \
--hive-drop-import-delims \
--fields-terminated-by ${fieldsSeparator} \
--lines-terminated-by '\n' \
--null-string '\\N' \
--null-non-string '\\N' \
--as-parquetfile \
-m 1

done