/*
 * This class converts and saves TSV files as parquet files
 *
 * Authors: Victor Weeks, Diego Batres, Josiah May
 */

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public final class TSVtoParquet {

    public static void main(String[] args) throws Exception {

	String dataLoc = "/HW4/sample_data";

	SparkSession spark = SparkSession
	    .builder()
	    .appName("TSVtoParquet")
	    .getOrCreate();

	Dataset msDataset = spark.read().format("csv")
	    .option("sep", "\t")
	    .option("inferSchema", "true")
	    .option("header", "true")
	    .load(dataLoc);

	/* 
	 * Field Formats:
	 * analysis_sample_rate (double)
	 * artist_terms (string array)
	 * bars_start (double array)
	 * beats_start (double array)
	 * danceability (double)
	 * duration (double)
	 * end_of_fade_in (double)
	 * energy (double)
	 * key (int)
	 * loudness (double)
	 * mode (int)
	 * sections_start (double array)
	 * segments_loudness_max (double array)
	 * segments_loudness_max_time (double array)
	 * segments_loudness_max_start (double array)
	 * segments_pitches (2D double array)
	 * segments_start (double array)
	 * segments_timbre (2D double array)
	 * start_of_fade_out (double)
	 * tatums_start (double array)
	 * tempo (double)
	 * time_signature (int)
	 * year (int)
	 */
	
	msDataset.select("analysis_sample_rate", "artist_terms", "bars_start",
			 "bears_start", "danceability", "duration", "end_of_fade_in",
			 "energy", "key", "loudness", "mode", "sections_start",
			 "segments_loudness_max", "segments_loudness_max_time",
			 "segments_loudness_max_start", "segments_pitches", "segments_start",
			 "segments_timbre", "start_of_fade_out", "tatums_start",
			 "tempo", "time_signature", "year")
	    .write()
	    .parquet("/HW4_data/msd-parquet");
					       
	spark.stop();
	
    }
    
}


