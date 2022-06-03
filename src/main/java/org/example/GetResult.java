package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static com.swoop.alchemy.spark.expressions.hll.functions.hll_cardinality;
import static org.apache.spark.sql.functions.col;

public class GetResult {

    private static  String hdfsPath = "hdfs://internship-hadoop105185:8120/mydata/";
    private static String date = "2022-06-03";
    private static String path = hdfsPath + "Day=" + date + "/*";
    /*
     *Ham main
     */
    public static void main(final String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.cj.jdbc.Driver")
                .option("url", "jdbc:mysql://localhost:3306/Intern2022")
                .option("dbtable", "mydata")
                .option("user", "namnp")
                .option("password", "12345678")
                .load();
        Dataset<Row> res = df.filter(col("Day").$eq$eq$eq(date));
        res.select(col("bannerId"),
                        hll_cardinality("guid_hll").as("Number of guids"))
                .show(false);
    }
}