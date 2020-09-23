package com.spark.problem;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class Sum {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("SUm of numbers in text file").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<String> javaRDD = jsc.textFile("D:\\TestData\\Spark\\numbers.txt");
        JavaRDD<String> numberRDD = javaRDD.flatMap(p -> Arrays.asList(p.split(",")).iterator());
        JavaRDD<Integer> intRDD = numberRDD.map(Integer::parseInt);
        int sum = intRDD.reduce((a, b) -> a + b);
        System.out.println("Sum : " + sum);

    }

}
