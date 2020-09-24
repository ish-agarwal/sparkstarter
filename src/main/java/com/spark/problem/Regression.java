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

import static org.apache.spark.sql.functions.col;

public class Regression {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Java Spark SQL Example").getOrCreate();

        Dataset<Row> old = spark.read().option("header", true).csv("D:\\TestData\\Spark\\Names.csv").alias("old");
        Dataset<Row> dsNew = spark.read().option("header", true).csv("D:\\TestData\\Spark\\Names_new.csv").alias("new");


        List<Column> allColumns = new ArrayList<>();
        allColumns.addAll(getDatasetColumns(old, "old"));
        allColumns.addAll(getDatasetColumns(dsNew, "new"));

        Dataset<Row> joinDs = old.join(dsNew, joinColumnsCondition(Arrays.asList("Id")), JoinType.LEFT.getJoinType());
        joinDs.select(allColumns.toArray(new Column[0])).show();

        Dataset<Row> filterDs = joinDs.filter(getFilter(Arrays.asList(old.columns())));
        filterDs.show();


    }

    private static Column getFilter(List<String> colList) {
        Column filter = null;
        for(String colName : colList) {
            filter = filter == null ? (col("old." + colName).isNull().or(col("new." + colName).isNull())).or(col("old." + colName).notEqual(col("new." + colName))) :
                filter.or((col("old." + colName).isNull().or(col("new." + colName).isNull())).or(col("old." + colName).notEqual(col("new." + colName))));
        }
        return filter;
    }


    private static List<Column> getDatasetColumns(Dataset<Row> dataset, String aliasIdentifier) {
        List<Column> colList = new ArrayList<>();
        Arrays.asList(dataset.columns()).forEach(colName -> colList.add(col(aliasIdentifier + "." + colName).alias(aliasIdentifier + "_" + colName)));
        return colList;
    }


    private static Column joinColumnsCondition(List<String> colForJoin) {
        Column joinCondition = null;
        for (String colName : colForJoin) {
            joinCondition = (joinCondition == null) ?  col("new." + colName).equalTo(col("old." + colName)) :
                    joinCondition.and(col("new." + colName).equalTo(col("old." + colName)));
        }
        return joinCondition;
    }


}
