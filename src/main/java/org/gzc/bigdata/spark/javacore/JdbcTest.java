package org.gzc.bigdata.spark.javacore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;


public class JdbcTest {
    public static void main(String[] args) {
        jdbcTest();
    }

    private static void jdbcTest() {
        SparkSession spark = SparkSession.builder()
                .appName("MyApp")
                .master("local")
                .getOrCreate();
        String url = "jdbc:mysql://localhost:12306/bsm";
        String table = "user";
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "GZCabc123");
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> jdbc = spark.read().jdbc(url, table, properties);
        jdbc.write().json("hdfs://localhost:9000/user/bsfit/data/user.json");

    }
}
