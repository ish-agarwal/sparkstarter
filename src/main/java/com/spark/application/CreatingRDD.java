package com.spark.application;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class CreatingRDD {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL Example").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //Parallelize
        List<String> strList = Arrays.asList("ish","viju","mom","dad","shin");
        JavaRDD<String> javaRDD = jsc.parallelize(strList);
        System.out.println("Array RDD count:" + javaRDD.count());

        //From the file - textFile
        JavaRDD<String> javaRDD1 = jsc.textFile("D:\\TestData\\Spark\\Text.txt");
        System.out.println("File RDD count:" + javaRDD1.count());
        javaRDD1.collect().forEach(p-> System.out.println(p));
        javaRDD1.map(p-> p.length());
        System.out.println("File RDD partitions:" + javaRDD1.partitions().size()); // Gives unexpected result for higher minPartitions count

        //From the file - wholeTextFile
        JavaPairRDD<String, String> pairRDD = jsc.wholeTextFiles("D:\\TestData\\Spark\\parks.csv");
        pairRDD.collect().forEach(p-> System.out.println(p._1()  + ":" + p._2()));

        //Read all text files from a directory into a single RDD
        JavaRDD<String> rddMulti = jsc.textFile("D:\\TestData\\Spark\\*");
        rddMulti.foreach(p-> System.out.println(p));

        //From the file - csv file
        JavaRDD<String> javaRDD2 = jsc.textFile("D:\\TestData\\Spark\\Names.csv");
        System.out.println("File RDD count:" + javaRDD2.count());
        javaRDD2.foreach(p -> System.out.println(p));
        JavaRDD<List<String>> splitRDD = javaRDD2.map(p -> Arrays.asList(p.split(",")));
        splitRDD.foreach(p -> System.out.println(p));
    }
}
