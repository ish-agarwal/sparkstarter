package com.spark.problem;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class MinimumRank {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL Example").getOrCreate();

        Dataset<Row> rank = spark.read().option("header", true).option("inferSchema", "true").csv("D:\\TestData\\Spark\\Ranks.csv");

        List<Column> allColumns = new ArrayList<>();
        allColumns.addAll(getDatasetColumns(rank));
        rank.groupBy("s", "b")
                .agg(expr("min(rank) as rank"))
                .orderBy("s", "b")
                .select(allColumns.toArray(new Column[0])).show();
    }

    private static List<Column> getDatasetColumns(Dataset<Row> dataset) {
        List<Column> colList = new ArrayList<>();
        Arrays.asList(dataset.columns()).forEach(colName -> {
            colList.add(col(colName));
        });
        return colList;
    }
}
