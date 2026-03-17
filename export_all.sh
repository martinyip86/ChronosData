#!/bin/bash
# 替换为你的本地数据库名
DB_NAME="market_data"
CONTAINER="clickhouse-server"

# 1. 导出建库语句
docker exec -i $CONTAINER clickhouse-client --query="SHOW CREATE DATABASE $DB_NAME" > full_dump.sql

# 2. 循环导出所有表和视图
TABLES=$(docker exec -i $CONTAINER clickhouse-client --database="$DB_NAME" --query="SHOW TABLES")

for table in $TABLES; do
    echo "--- Exporting: $table ---"
    # 导出表结构
    docker exec -i $CONTAINER clickhouse-client --database="$DB_NAME" --query="SHOW CREATE TABLE $table" >> full_dump.sql
    echo ";" >> full_dump.sql
    
    # 判断是否是 View (View 不需要导数据)
    IS_VIEW=$(docker exec -i $CONTAINER clickhouse-client --database="$DB_NAME" --query="SELECT count() FROM system.tables WHERE database='$DB_NAME' AND name='$table' AND engine LIKE '%View'")
    
    if [ "$IS_VIEW" -eq "0" ]; then
        echo "INSERT INTO $table FORMAT Native" >> full_dump.sql
        docker exec -i $CONTAINER clickhouse-client --database="$DB_NAME" --query="SELECT * FROM $table FORMAT Native" >> full_dump.sql
    fi
done