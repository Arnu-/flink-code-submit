package me.arnu.flink.demo;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class FlinkSqlFromConfDemo {
    public static void main(String[] args) throws Exception {
        FlinkSqlFromConfDemo demo = new FlinkSqlFromConfDemo();
        int jobId = -1;
        try {
            jobId = Integer.parseInt(args[0]);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(-1);
        }
        demo.runFlinkSqlJob(jobId);
    }

    public FlinkSqlFromConfDemo() {

    }

    public void runFlinkSqlJob(int jobId) throws Exception {
        String sqlDDL = "select id, name, value, type from flink_ddl where job_id=? and valid=1";
        String sqlQuery = "select id, value, `index` from flink_query where job_id=? and valid=1";
        ComboPooledDataSource dataSource = new ComboPooledDataSource("flinksql");
        String sourceDDL = "";
        String sourceTableName = "";
        String sinkDDL = "";
        String sinkTableName = "";
        String[] querySql = new String[10];
        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(sqlDDL);
            ps.setInt(1, jobId);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int type = rs.getInt("type");
                String value = rs.getString("value");
                String tableName = rs.getString("name");
                if (type == 1) {
                    sourceDDL = value;
                    sourceTableName = tableName;
                } else if (type == 2) {
                    sinkDDL = value;
                    sinkTableName = tableName;
                }
            }
            rs.close();
            ps.close();
            ps = conn.prepareStatement(sqlQuery);
            ps.setInt(1, jobId);
            rs = ps.executeQuery();
            while (rs.next()) {
                int index = rs.getInt("index");
                String value = rs.getString("value");
                if (index > -1) {
                    querySql[index] = value;
                }
            }
            rs.close();
            ps.close();
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }

        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
        bsTableEnv.executeSql(sourceDDL);
        bsTableEnv.executeSql(sinkDDL);

        Table table = bsTableEnv.sqlQuery(querySql[0]);

        table.executeInsert(sinkTableName);
//        DataStream<Row> dsRow = bsTableEnv.toAppendStream(table, Row.class);
//        DataStream<Tuple2<Boolean, Row>> dsRow = bsTableEnv.toRetractStream(table, Row.class);
//        dsRow.print();
        bsTableEnv.execute("执行");
//        bsEnv.execute("执行");
    }
}
