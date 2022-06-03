package org.example;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.date_sub;
import static com.swoop.alchemy.spark.expressions.hll.functions.*;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main(final String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.3.68.20:9092,"
                        + " 10.3.68.21:9092, 10.3.68.23:9092,"
                        + " 10.3.68.26:9092, 10.3.68.28:9092,"
                        + " 10.3.68.32:9092, 10.3.68.47:9092,"
                        + " 10.3.68.48:9092, 10.3.68.50:9092,"
                        + " 10.3.68.52:9092")
                .option("subscribe", "rt-queue_1")
                .load();
        Dataset<Row> value = df.selectExpr("CAST(value AS STRING)");
        value = value.select(split(col("value"), "\t")
                                .getItem(0).cast("long")
                                .cast("timestamp").as("time"),
                        split(col("value"), "\t")
                                .getItem(6)
                                .cast("String")
                                .as("guid"),
                        split(col("value"), "\t")
                                .getItem(4)
                                .cast("String")
                                .as("bannerId"))
                .withColumn("Date", split(col("time"), " ")
                        .getItem(0))
                .withColumn("Hour", split(col("time"), " ")
                        .getItem(1))
                .drop(col("time"));

        value = value.withColumn("Day",
                        when(col("Hour").geq("06:00:00"), col("Date"))
                                .when(col("Hour").lt("06:00:00"), date_sub(col("Date"), 1)))
                .drop(col("Date"))
                .drop("Hour");
        Dataset<Row> prevDF = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/Intern2022")
                .option("dbtable", "mydata")
                .option("user", "namnp")
                .option("password", "12345678")
                .load();
        Dataset<Row> res=prevDF.union(value);
        res=res.groupBy(col("Day"), col("bannerId")).agg(sum("guid_hll").as("guid_hll"));
        try {
            res.coalesce(1).writeStream()
                    .trigger(Trigger.ProcessingTime("5 minutes"))
                    .outputMode("overwrite")
                    .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchDF, batchId) ->
                            batchDF.groupBy(col("Day"), col("bannerId"))
                                    .agg(hll_init_agg("guid")
                                            .as("guid_hll"))
                                    .groupBy(col("Day"), col("bannerId"))
                                    .agg(hll_merge("guid_hll")
                                            .as("guid_hll"))
                                    .select(col("Day"), col("bannerId"),
                                            hll_cardinality("guid_hll").as("guid_hll"))
                                    .write()
                                    .format("jdbc")
                                    .option("driver", "com.mysql.cj.jdbc.Driver")
                                    .option("url", "jdbc:mysql://localhost:3306/Intern2022")
                                    .option("dbtable", "mydata")
                                    .option("user", "namnp")
                                    .option("password", "12345678")
                                    .mode("append")
                                    .save()
                    )
                    .start().awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
