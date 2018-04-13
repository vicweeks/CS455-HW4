/*
 * CS455: HW4 Term Project
 * Authors: Victor Weeks & Diego Batres
 */

package src.main.java;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SchemaRDD; // used by Spark ML

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

// TODO

public final class HW4 {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {
      
    if (args.length < 1) {
      System.err.println("Usage: HW4 <file>");
      System.exit(1);
    }
      
    SparkSession spark = SparkSession
      .builder()
      .appName("HW4")
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
    JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    counts.coalesce(1).saveAsTextFile("/HW4/Example/WordCountOutput");
    
    spark.stop();
  }
}
