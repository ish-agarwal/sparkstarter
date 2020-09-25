package com.spark.problem;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Scanner;

public class MovieOfTheYear {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Movies of the year").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Scanner input = new Scanner(System.in);

        System.out.print("Enter the year: ");
        int number = input.nextInt();

        JavaRDD<String> javaRDD = jsc.textFile("D:\\TestData\\Spark\\MoviesInventory.txt");
//        javaRDD.collect().forEach(p -> System.out.println(p));

        JavaPairRDD<Integer, String> moviePair = javaRDD.mapToPair(p -> {
            String[] strArr = p.split(" ");
            Integer key = Integer.parseInt(strArr[strArr.length - 1]);
            String value = String.join(" ", Arrays.copyOf(strArr, strArr.length-1));
            return new Tuple2<>(key, value);
        });

        Iterable<String> movies = moviePair.groupByKey().collectAsMap().get(number);
        if(movies != null) {
            movies.forEach(p -> System.out.println(p));
            System.out.println("--------------------");
            movies.forEach(p -> System.out.println(p + " : " + p.length() ));
        } else {
            System.out.println("Shoot ! no movie");
        }

    }
}
