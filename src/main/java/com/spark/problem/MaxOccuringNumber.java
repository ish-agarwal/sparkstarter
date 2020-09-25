package com.spark.problem;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

public class MaxOccuringNumber {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Max occurring number").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaRDD<String> javaRDD = jsc.textFile("D:\\TestData\\Spark\\RandomNumber.txt");
//        javaRDD.collect().forEach(p -> System.out.println(p));

        List<Tuple2<Long, String>> numberCount = new ArrayList<>();
        javaRDD.countByValue().entrySet().forEach(entry -> numberCount.add(new Tuple2<>(entry.getValue(), entry.getKey())));

        JavaPairRDD<Long, String> countPair = jsc.parallelizePairs(numberCount);
        Tuple2<Long, Iterable<String>> maxCount = countPair.groupByKey().max(new TupleComparator());

        maxCount._2().forEach(p -> System.out.println(p + " : " + maxCount._1()));
    }

    public static class TupleComparator implements Comparator<Tuple2<Long, Iterable<String>>>, Serializable {
        @Override
        public int compare(Tuple2<Long, Iterable<String>> x, Tuple2<Long, Iterable<String>> y) {
            return Long.compare(x._1(), y._1());
        }
    }

}
