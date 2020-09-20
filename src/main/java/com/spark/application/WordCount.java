package com.spark.application;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Wordcount").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<String> javaRDD1 = jsc.textFile("D:\\TestData\\Spark\\Text.txt");
        javaRDD1.flatMap(p -> Arrays.asList(p.split(" ")).iterator()).countByValue().entrySet().forEach(p -> System.out.println(p.getKey() + ":" + p.getValue()));

    }
}
