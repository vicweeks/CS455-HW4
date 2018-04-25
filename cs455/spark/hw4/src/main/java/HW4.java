/*
 * CS455: HW4 Term Project
 * Authors: Victor Weeks, Diego Batres, Josiah May
 */


import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;


public final class HW4 {

  public static void main(String[] args) throws Exception {

    String dataLoc;

    if (args.length < 1) {
      dataLoc = "/HW4/MSD_data/9*";
    } else {
      dataLoc = args[0];
    }

    //FindMostPopularGenre test1 = new FindMostPopularGenre(dataFull);
    SparkSession spark = SparkSession
        .builder()
        .appName("HW4")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    Dataset dataFull = spark.read().format("csv")
        .option("sep", "\t")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(dataLoc);

    Dataset dataFiltered = dataFull.filter("year > 0 AND tempo > 0 AND time_signature > 0")
        .select("artist_terms", "duration", "end_of_fade_in",
            "key", "loudness", "mode", "start_of_fade_out", "tempo",
            "time_signature", "year", "segments_start", "segments_timbre",
            "tatums_start", "bars_start", "beats_start",
            "segments_loudness_max", "segments_pitches", "sections_start");

    //FindMostPopularGenre test1 = new FindMostPopularGenre(dataFull);
    //test1.run();

    FindTheGenre classify = new FindTheGenre(dataFiltered);
    classify.run();

    spark.stop();
  }

}
