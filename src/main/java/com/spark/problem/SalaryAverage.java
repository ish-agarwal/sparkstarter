package com.spark.problem;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SalaryAverage {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Calculate Salary").getOrCreate();

        Dataset<Row> salary = spark.read().option("header", true).option("inferSchema", "true").csv("D:\\TestData\\Spark\\SalaryDetails.csv").alias("salaryTable");
//        salary.show();

        Dataset<Row> withAvgSalary = salary.join(salary.groupBy("city").agg(avg("salary").as("salary_avg")), "city").alias("avgSalary");
//        withAvgSalary.show();

        Dataset<Row> overallAvg = withAvgSalary.join(withAvgSalary.agg(avg("avgSalary.Salary").as("overall_avg_salary")))
                .withColumn("average%diff", expr("((avgSalary.salary_avg - overall_avg_salary)/overall_avg_salary)*100"))
                .withColumn("emp%Diff", expr("((avgSalary.salary - overall_avg_salary)/overall_avg_salary)*100"))
                .withColumn("empCityAvf%diff", expr("((avgSalary.salary - avgSalary.salary_avg)/avgSalary.salary_avg)*100"));
        overallAvg.show();

    }

    private static Column joinColumnsCondition(List<String> colForJoin) {
        Column joinCondition = null;
        for (String colName : colForJoin) {
            joinCondition = (joinCondition == null) ?  col("salaryTable." + colName).equalTo(col(colName)) :  joinCondition.and(col("salaryTable." + colName).equalTo(col(colName)));
        }
        return joinCondition;
    }
}
