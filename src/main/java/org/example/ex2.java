package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;

public class ex2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("EX 2 SALES").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /**Read the file*/
        JavaRDD<String> rddFile = sc.textFile("ventes.txt");
        rddFile.persist(StorageLevel.MEMORY_ONLY());

        /*Question 1: */
        JavaPairRDD<String,Double> rddPairs = rddFile.mapToPair(word -> {
            String[] wordArr = word.split(" ");
            return new Tuple2<>(wordArr[1],Double.parseDouble(wordArr[3]));
        });
        List<Tuple2<String,Double>> wordCount = rddPairs.reduceByKey((a, b) -> a+b).collect();

        for (Tuple2<String,Double> wrd : wordCount){
            System.out.println(wrd._1()+" : "+wrd._2());
        }

        /*Question 2: */
        JavaPairRDD<String,Double> rddPairsDate = rddFile.mapToPair(word -> {
            String[] wordArr = word.split(" ");
            String date = wordArr[0].split("-")[2];
            return new Tuple2<>(wordArr[1]+date,Double.parseDouble(wordArr[3]));
        });
        List<Tuple2<String,Double>> wordCountDate = rddPairsDate.reduceByKey((a, b) -> a+b).collect();

        for (Tuple2<String,Double> wrd : wordCountDate){
            System.out.println(wrd._1()+" : "+wrd._2());
        }
    }
}