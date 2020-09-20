package com.spark.application;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkDriverProgram {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL Example").getOrCreate();
        System.out.println("Version : " + spark.version());


        //Spark 1.0
        SparkConf sc = new SparkConf().setAppName("Driver Program").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        System.out.println("Partitions 1.0 : " + jsc.defaultMinPartitions());
        jsc.close();

        //Spark2.0
        JavaSparkContext jsc2 = JavaSparkContext.fromSparkContext(spark.sparkContext());
        System.out.println("Partitions 2.0 : " + jsc2.defaultMinPartitions());
        jsc2.close();
    }

}
