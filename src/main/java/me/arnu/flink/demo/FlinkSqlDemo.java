package me.arnu.flink.demo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSqlDemo {
    public static void main(String[] args) throws Exception {
        /*String ddl = "CREATE TABLE user_follow_user (\n" +
                "  ftime datetime,\n" +
                "  env varchar(20), \n" +
                "  from_user_id varchar(20), \n" +
                "  to_user_id varchar(20), \n" +
                "  opt varchar(20), \n" +
                "  user_type int, \n" +
                "  millisecond  bigint\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',       \n" +
                "\n" +
                "  'connector.version' = 'universal',     -- required: valid connector versions are\n" +
                "                                    -- \"0.8\", \"0.9\", \"0.10\", \"0.11\", and \"universal\"\n" +
                "\n" +
                "  'connector.topic' = 'vm-topic-1', -- required: topic name from which the table is read\n" +
                "\n" +
                "  -- required: specify the Kafka server connection string\n" +
                "  'connector.properties.bootstrap.servers' = 'vm1:9091,vm2:9092,vm3:9092',\n" +
                "  -- required for Kafka source, optional for Kafka sink, specify consumer group\n" +
                "  'connector.properties.group.id' = 'flink-sql-group-1',\n" +
                "  -- optional: valid modes are \"earliest-offset\", \"latest-offset\", \"group-offsets\", \"specific-offsets\" or \"timestamp\"\n" +
                "  'connector.startup-mode' = 'group-offsets',\n" +
                "\n" +
                "  -- optional: used in case of startup mode with specific offsets\n" +
                "  -- 'connector.specific-offsets' = 'partition:0,offset:42;partition:1,offset:300',\n" +
                "\n" +
                "  -- optional: used in case of startup mode with timestamp\n" +
                "  -- 'connector.startup-timestamp-millis' = '1578538374471',\n" +
                "\n" +
                "  -- 'connector.sink-partitioner' = '...',  -- optional: output partitioning from Flink's partitions \n" +
                "                                         -- into Kafka's partitions valid are \"fixed\" \n" +
                "                                         -- (each Flink partition ends up in at most one Kafka partition),\n" +
                "                                         -- \"round-robin\" (a Flink partition is distributed to \n" +
                "                                         -- Kafka partitions round-robin)\n" +
                "                                         -- \"custom\" (use a custom FlinkKafkaPartitioner subclass)\n" +
                "\n" +
                "  -- optional: used in case of sink partitioner custom\n" +
                "  -- 'connector.sink-partitioner-class' = 'org.mycompany.MyPartitioner',\n" +
                "  \n" +
                "  'format.type' = 'csv',                 -- required: Kafka connector requires to specify a format,\n" +
                "  -- ...                                    -- the supported formats are 'csv', 'json' and 'avro'.\n" +
                "                                         -- Please refer to Table Formats section for more details.\n" +
                ")";*/
        String ddl = "CREATE TABLE user_follow_user (\n" +
                "  ftime STRING,\n" +
                "  env STRING, \n" +
                "  from_user_id STRING, \n" +
                "  to_user_id STRING, \n" +
                "  opt int, \n" +
                "  user_type int, \n" +
                "  millisecond  bigint,\n" +
                "  imp_date  bigint\n" +
                ") WITH (\n" +
                "  'connector.type' = 'kafka',       \n" +

                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = 'vm-topic-1', \n" +

                "  'connector.properties.bootstrap.servers' = 'vm1:9091,vm2:9092,vm3:9092',\n" +
                "  'connector.properties.group.id' = 'flink-sql-group-1',\n" +
                "  'connector.startup-mode' = 'group-offsets',\n" +
                "  'format.type' = 'csv'\n" +
                ")";
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsTableEnv.executeSql(ddl);
        /*String sinkDDL = "CREATE TABLE MyUserTable (\n" +
                "  ...\n" +
                ") WITH (\n" +
                "  'connector.type' = 'jdbc', -- required: specify this table type is jdbc\n" +
                "  \n" +
                "  'connector.url' = 'jdbc:mysql://localhost:3306/flink-test', -- required: JDBC DB url\n" +
                "  \n" +
                "  'connector.table' = 'jdbc_table_name',  -- required: jdbc table name\n" +
                "\n" +
                "  -- optional: the class name of the JDBC driver to use to connect to this URL.\n" +
                "  -- If not set, it will automatically be derived from the URL.\n" +
                "  'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "\n" +
                "  -- optional: jdbc user name and password\n" +
                "  'connector.username' = 'name',\n" +
                "  'connector.password' = 'password',\n" +
                "  \n" +
                "  -- **followings are scan options, optional, used when reading from a table**\n" +
                "\n" +
                "  -- optional: SQL query / prepared statement.\n" +
                "  -- If set, this will take precedence over the 'connector.table' setting\n" +
                "  'connector.read.query' = 'SELECT * FROM sometable',\n" +
                "\n" +
                "  -- These options must all be specified if any of them is specified. In addition,\n" +
                "  -- partition.num must be specified. They describe how to partition the table when\n" +
                "  -- reading in parallel from multiple tasks. partition.column must be a numeric,\n" +
                "  -- date, or timestamp column from the table in question. Notice that lowerBound and\n" +
                "  -- upperBound are just used to decide the partition stride, not for filtering the\n" +
                "  -- rows in table. So all rows in the table will be partitioned and returned.\n" +
                "\n" +
                "  'connector.read.partition.column' = 'column_name', -- optional: the column name used for partitioning the input.\n" +
                "  'connector.read.partition.num' = '50', -- optional: the number of partitions.\n" +
                "  'connector.read.partition.lower-bound' = '500', -- optional: the smallest value of the first partition.\n" +
                "  'connector.read.partition.upper-bound' = '1000', -- optional: the largest value of the last partition.\n" +
                "\n" +
                "  -- optional, Gives the reader a hint as to the number of rows that should be fetched\n" +
                "  -- from the database when reading per round trip. If the value specified is zero, then\n" +
                "  -- the hint is ignored. The default value is zero.\n" +
                "  'connector.read.fetch-size' = '100',\n" +
                "\n" +
                "  -- **followings are lookup options, optional, used in temporary join**\n" +
                "\n" +
                "  -- optional, max number of rows of lookup cache, over this value, the oldest rows will\n" +
                "  -- be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any\n" +
                "  -- of them is specified. Cache is not enabled as default.\n" +
                "  'connector.lookup.cache.max-rows' = '5000',\n" +
                "\n" +
                "  -- optional, the max time to live for each rows in lookup cache, over this time, the oldest rows\n" +
                "  -- will be expired. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of\n" +
                "  -- them is specified. Cache is not enabled as default.\n" +
                "  'connector.lookup.cache.ttl' = '10s',\n" +
                "\n" +
                "  'connector.lookup.max-retries' = '3', -- optional, max retry times if lookup database failed\n" +
                "\n" +
                "  -- **followings are sink options, optional, used when writing into table**\n" +
                "\n" +
                "  -- optional, flush max size (includes all append, upsert and delete records),\n" +
                "  -- over this number of records, will flush data. The default value is \"5000\".\n" +
                "  'connector.write.flush.max-rows' = '5000',\n" +
                "\n" +
                "  -- optional, flush interval mills, over this time, asynchronous threads will flush data.\n" +
                "  -- The default value is \"0s\", which means no asynchronous flush thread will be scheduled.\n" +
                "  'connector.write.flush.interval' = '2s',\n" +
                "\n" +
                "  -- optional, max retry times if writing records to database failed\n" +
                "  'connector.write.max-retries' = '3'\n" +
                ")";*/
        String sinkDDL = "CREATE TABLE user_fans (\n" +
                "  `user_id` varchar(30) NOT NULL,\n" +
                "  `fans_count` BIGINT ,\n" +
                "  `update_time` bigint \n" +
                ") WITH (\n" +
                "  'connector.type' = 'jdbc',\n" +
                "  'connector.url' = 'jdbc:mysql://localhost:3306/springbootdb',\n" +
                "  'connector.table' = 'user_fans',\n" +
                "  'connector.driver' = 'com.mysql.jdbc.Driver',\n" +
                "  'connector.username' = 'spring',\n" +
                "  'connector.password' = 'springboot',\n" +
                "  'connector.write.flush.max-rows' = '1000',\n" +
                "  'connector.write.flush.interval' = '20s',\n" +
                "  'connector.write.max-retries' = '3'\n" +
                ")";

        bsTableEnv.executeSql(sinkDDL);

        String query = "Select to_user_id as user_id, count(1) as fans_count, max(millisecond) as update_time" +
                " from user_follow_user" +
                " where opt=1 " +
                " group by to_user_id";
        Table table = bsTableEnv.sqlQuery(query);

        table.executeInsert("user_fans");
//        DataStream<Row> dsRow = bsTableEnv.toAppendStream(table, Row.class);
//        DataStream<Tuple2<Boolean, Row>> dsRow = bsTableEnv.toRetractStream(table, Row.class);
//        dsRow.print();
        bsTableEnv.execute("执行");
//        bsEnv.execute("执行");
    }
}
