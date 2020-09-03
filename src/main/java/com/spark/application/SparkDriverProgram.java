package com.spark.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriverProgram {
    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf()
//                .setAppName("Example Spark App")
//                .setMaster("local[*]"); // Delete this line when submitting to a cluster
//        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
//        JavaRDD<String> stringJavaRDD = sparkContext.textFile("D:\\TestData\\Spark\\nationalparks.csv");
//        JavaRDD<Object> rowRDD = stringJavaRDD.map(RowFactory::create);
//        System.out.println("Number of lines in file = " + stringJavaRDD.count());

        SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL Example").getOrCreate();
        Dataset<Row> df = spark.read().csv("D:\\TestData\\Spark\\nationalparks.csv");
        df.show();
    }

}
