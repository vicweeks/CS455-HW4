/*
 * CS455: HW4 Term Project
 * Authors: Victor Weeks, Diego Batres, Josiah May
 */

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.substring_index;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;

import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;

public final class HW4 {

    public static void main(String[] args) throws Exception {

	String dataLoc;
      
	if (args.length < 1) {
	    dataLoc = "/HW4/sample_data";
	} else {
	    dataLoc = args[0];
	}
      
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
	//FindSectionsInfo test2 = new FindSectionsInfo(dataFull);
	//test2.run();
	//test1.run();
	
	FindTheGenre classify = new FindTheGenre(dataFiltered);
	classify.run();

	spark.stop();
  }
    
}
