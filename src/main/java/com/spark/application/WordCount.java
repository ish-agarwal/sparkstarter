package com.spark.application;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.SparkSession;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().master("local[*]").appName("Wordcount").getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        //Text File
        JavaRDD<String> javaRDD1 = jsc.textFile("D:\\TestData\\Spark\\Text.txt");
        javaRDD1.flatMap(p -> Arrays.asList(p.split(" ")).iterator()).countByValue().entrySet().forEach(p -> System.out.println(p.getKey() + ":" + p.getValue()));

        //DOCX file
        JavaPairRDD<String, PortableDataStream> docRDD = jsc.binaryFiles("D:\\TestData\\Spark\\Sampe.docx");
        docRDD.flatMap(p -> Arrays.asList(extractContentUsingParser(p._1()).split(" ")).iterator()).countByValue().entrySet().forEach(p -> System.out.println(p.getKey() + ":" + p.getValue()));

    }

    public static String extractContentUsingParser(String fileName) throws IOException, TikaException, SAXException {
        Parser parser = new AutoDetectParser();
        ContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        ParseContext context = new ParseContext();
        try (InputStream stream = new FileInputStream(new File(fileName.replace("file:/", "")))) {
            parser.parse(stream, handler, metadata, context);
            return handler.toString();
        }
    }

}