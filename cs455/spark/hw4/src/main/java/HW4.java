/*
 * CS455: HW4 Term Project
 * Authors: Victor Weeks, Diego Batres, Josiah May
 */

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.util.regex.Pattern;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.substring_index;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RowFactory;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;

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

	Dataset dataSub = dataFull.select("artist_terms", "danceability", "duration", "end_of_fade_in",
			 "energy", "key", "loudness", "mode", "start_of_fade_out", "tempo",
				       "time_signature", "year");

       	Dataset data = getFirstTerms(dataSub, "artist_terms", DataTypes.StringType).as(Encoders.bean(Song.class));

	data.printSchema();

	
	StructType libsvmSchema = new StructType().add("label", "String").add("features", new VectorUDT());  

	Dataset dsLibsvm = spark.createDataFrame(
						 data.javaRDD().map(new Function<Song, Row>() {  
						      public Row call(Song s) {							  
							  String label = s.getArtist_Terms();
							  double[] features = {s.getDanceability(), s.getDuration(), s.getEnd_Of_Fade_In(), s.getEnergy(), s.getKey(), s.getLoudness(), s.getMode(),
									       s.getStart_Of_Fade_Out(), s.getTempo(), s.getTime_Signature(), s.getYear()};
							  Vector currentRow = Vectors.dense(features);  
							  return RowFactory.create(label, currentRow);  
						      }  
						  }), libsvmSchema);   

	//dsLibsvm.printSchema();
	//dsLibsvm.show();

	// Trains a k-means model.
	KMeans kmeans = new KMeans().setK(10).setSeed(1L);
	KMeansModel model = kmeans.fit(dsLibsvm);
	
	// Make predictions
	Dataset<Row> predictions = model.transform(dsLibsvm);

	// Evaluate clustering by computing Within Set Sum of Squared Errors
	double WSSSE = model.computeCost(dsLibsvm);
	System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

	// Shows the result.
	Vector[] centers = model.clusterCenters();
	System.out.println("Cluster Centers: ");
	for (Vector center: centers) {
	    System.out.println(center);
	}
	
	
	spark.stop();
  }

	/**
	 * Splits a string into an array and removes all chars that were part of the array setup ( " \ [ ] )
	 * It changes the array type to the values given
	 * @param data Dataset to read
	 * @param columnName column to change
	 * @param dataType the type of the array
	 * @return The new dataset with the changed info
	 */
	private static Dataset<Row> getSplitTerms(Dataset<Row> data, String columnName, DataType dataType) {
		return getSplitTerms(data, columnName, columnName, dataType);
	}

  /**
   * Splits a string into an array and removes all chars that were part of the array setup ( " \ [ ] )
   * and saves it to a new column
   * It changes the array type to the values given
   * @param data Dataset to read
   * @param columnNameOriginal column to change
   * @param columnNameNew the new column named
   * @param dataType the type of the array
   * @return The new dataset with the changed info
   */
  private static Dataset<Row> getSplitTerms(Dataset<Row> data, String columnNameOriginal, String columnNameNew, DataType dataType) {
    return data.withColumn(columnNameOriginal ,split(
        regexp_replace(data.col(columnNameNew), "[\\\\\"\\[\\]]+", ""), ", ").cast(DataTypes.createArrayType( dataType)));
  }
    
  /**
   * Gets the first element of a string[] and returns an array of just that element
   * It need to be an array for the Machine learning models
   * @param data Dataset to read
   * @param columnName column to change
   * @param dataType the type of the array
   * @return The new dataset with the changed info
   */
  private static Dataset<Row> getFirstTerms(Dataset<Row> data, String columnName, DataType dataType) {
    return getFirstTerms(data, columnName, columnName, dataType);
  }

  /**
   * Gets the first element of a string[] and returns an array of just that element and saves it to a new column
   * It need to be an array for the Machine learning models
   * @param data Dataset to read
   * @param columnNameOriginal column to change
   * @param columnNameNew the new column named
   * @param dataType the type of the array
   * @return The new dataset with the changed info
   */
  private static Dataset<Row> getFirstTerms(Dataset<Row> data, String columnNameOriginal, String columnNameNew, DataType dataType) {
    return data.withColumn(columnNameOriginal ,split(substring_index(
        regexp_replace(data.col(columnNameNew), "[\\\\\"\\[\\]]+", ""), ", ", 1), ", " ).cast(DataTypes.createArrayType( dataType)));
  }

}
