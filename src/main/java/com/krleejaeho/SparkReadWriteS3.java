package com.krleejaeho;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


import java.util.ArrayList;
import java.util.List;

/**
 * Computes an approximation to pi
 * Usage: com.krleejaeho.JavaSparkPi [slices]
 */
public final class SparkReadWriteS3 {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("com.krleejaeho.JavaSparkPi");
        sparkConf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkReadWriteS3")
                .master("cluster")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read()
                .format("csv")
                .option("delimiter", args[0])
                .option("header", "true")
                .load("s3");

        df.show();

        df.printSchema();

    }
}