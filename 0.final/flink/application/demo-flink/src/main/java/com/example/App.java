package com.example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws Exception {
        // 1. Khởi tạo TableEnvironment (streaming)
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tEnv = TableEnvironment.create(settings);


        // 2. Source Kafka Debezium JSON (tạo bảng ảo trong Flink)
        String kafkaSourceDdl = "CREATE TABLE kafka_customer (\n" +
                "    `before` ROW<id INT, name STRING, email STRING, created_at TIMESTAMP(3)>,\n" +
                "    `after`  ROW<id INT, name STRING, email STRING, created_at TIMESTAMP(3)>,\n" +
                "    op STRING,\n" +
                "    ts_ms TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'demo.public.customer',\n" +
                "    'properties.bootstrap.servers' = 'broker:19092',\n" +
                "    'properties.group.id' = 'demo-flink-1',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format' = 'json',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";

        // 3. Sink PostgreSQL
        String postgresSinkDdl = "CREATE TABLE pg_customer_event (\n" +
                "    op STRING,\n" +
                "    id INT,\n" +
                "    name STRING,\n" +
                "    email STRING,\n" +
                "    created_at TIMESTAMP(3),\n" +
                "    ts_ms TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:postgresql://postgresdb:5432/demo',\n" +
                "    'table-name' = 'public.output_flink',\n" +
                "    'username' = 'postgres',\n" +
                "    'password' = 'postgres',\n" +
                "    'driver' = 'org.postgresql.Driver'\n" +
                ")";

        // 4. INSERT SELECT trực tiếp
        String insertSql = "INSERT INTO pg_customer_event\n" +
                "SELECT\n" +
                "    op,\n" +
                "    CASE WHEN op = 'd' THEN `before`.id ELSE `after`.id END AS id,\n" +
                "    CASE WHEN op = 'd' THEN `before`.name ELSE `after`.name END AS name,\n" +
                "    CASE WHEN op = 'd' THEN `before`.email ELSE `after`.email END AS email,\n" +
                "    CASE WHEN op = 'd' THEN `before`.created_at ELSE `after`.created_at END AS created_at,\n" +
                "    ts_ms\n" +
                "FROM kafka_customer";

        // 5. Thực thi
        tEnv.executeSql(kafkaSourceDdl);
        tEnv.executeSql(postgresSinkDdl);
        tEnv.executeSql(insertSql);

        // TableResult result = tEnv.executeSql(insertSql);

        // // 6. Block thread để job chạy
        // result.getJobClient()
        //         .ifPresent(jobClient -> {
        //             try {
        //                 jobClient.getJobExecutionResult().get();
        //             } catch (Exception e) {
        //                 throw new RuntimeException(e);
        //             }
        //         });
    }
}
