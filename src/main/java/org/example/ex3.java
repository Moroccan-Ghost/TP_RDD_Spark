package org.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class ex3 implements Serializable {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("EX 2 SALES").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /**Read the file*/
        JavaRDD<String> rddFile = sc.textFile("2020.csv");

        JavaRDD<Double> weatherTemp = rddFile.map(line -> {
            String[] fields = line.split(",");
            return Double.parseDouble(fields[3]);
        });

        double minTemp = weatherTemp.min(Comparator.naturalOrder());
        double maxTemp = weatherTemp.max(Comparator.naturalOrder());
        double avgMinTemp = weatherTemp.reduce(Double::sum) / weatherTemp.count();
        double avgMaxTemp = weatherTemp.reduce(Double::sum) / weatherTemp.count();

        /*Top 5 stations*/
        JavaPairRDD<String,Double> weatherStations = rddFile.mapToPair(line->{
            String[] fields = line.split(",");
            return new Tuple2<>(fields[0],Double.parseDouble(fields[3]));
        });

        List<Tuple2<String,Double>> topHottestStations = weatherStations.top(5,Comparator.comparingDouble(Tuple2::_2));
        List<Tuple2<Double, String>> topColdestStations = weatherStations.mapToPair(Tuple2::swap).sortByKey(false).take(5);

        System.out.println("Temp Max Moyenne "+avgMaxTemp);
        System.out.println("Temp Min Moyenne "+avgMinTemp);
        System.out.println("Temp Max "+maxTemp);
        System.out.println("Temp Min "+minTemp);
        System.out.println("Top 5 Hottest Stations");
        topHottestStations.forEach(station-> System.out.println(station._1 + " " + station._2));
        System.out.println("Top 5 Coldest Stations");
        topColdestStations.forEach(station-> System.out.println(station._2+ " " +station._1 ));



    }
}
