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
                .master("yarn")
                .getOrCreate();

        System.out.println("file location: " + args[0]);
        Dataset<Row> df = sparkSession.read()
                .format("csv")
                .option("delimiter", ",")
                .option("header", "true")
                .load(args[0]);

        df.show();

        df.printSchema();

    }
}