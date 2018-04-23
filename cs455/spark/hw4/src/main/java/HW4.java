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
import org.apache.spark.mllib.util.MLUtils;
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
	    dataLoc = "/HW4/sample_data/";
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


	//FindTheGenre finalGoal = new FindTheGenre(dataFull);
	//finalGoal.run();

	/*
	Dataset termsData = getFirstNterms(dataFull, "artist_terms",
					   "artist_terms", DataTypes.StringType, 5);

	Dataset splitTerms = getSplitTerms(dataFull, "artist_terms", DataTypes.StringType);

	//splitTerms.show(5);
	
	FPGrowthModel model = new FPGrowth()
	    .setItemsCol("artist_terms")
	    .setMinSupport(0.2)
	    .setMinConfidence(0.6)
	    .fit(splitTerms);

	// Display frequent itemsets.
	model.freqItemsets().show();
	model.freqItemsets().orderBy(col("freq").desc()).write().format("json").mode(SaveMode.Overwrite).save("/HW4_output/FPgrowth/freqItems");
	// Display generated association rules.
	model.associationRules().show();
	model.associationRules().orderBy(col("confidence").desc()).write().mode(SaveMode.Overwrite).format("json").save("/HW4_output/FPgrowth/rules");
	// transform examines the input items against all the association rules and summarize the
	// consequents as prediction
	//model.transform(splitTerms).show();
	*/

	
	FindMostPopularGenre test1 = new FindMostPopularGenre(dataFull);
	FindSectionsInfo test2 = new FindSectionsInfo(dataFull);
	test2.run();
	//test1.run();

	FindTheGenre test3 = new FindTheGenre(dataFull);
			test3.run();


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
    return data.withColumn(columnNameOriginal ,substring_index(
        regexp_replace(data.col(columnNameNew), "[\\\\\"\\[\\]]+", ""), ", ", 1).cast( dataType));
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
  private static Dataset<Row> getFirstNterms(Dataset<Row> data, String columnNameOriginal, String columnNameNew, DataType dataType, int numberOfItems) {
    return data.withColumn(columnNameOriginal ,substring_index(
        regexp_replace(data.col(columnNameNew), "[\\\\\"\\[\\]]+", ""), ", ", numberOfItems).cast( dataType));
  }

  private static Dataset<Row> removeArrays(Dataset<Row> data, String columnNameOriginal, String columnNameNew, DataType dataType) {
    return data.withColumn(columnNameOriginal , regexp_replace(data.col(columnNameNew), "[\\\\\"\\[\\]]+", "").cast( dataType));
  }

  private static double[] combineDoubles(double[] array1, double[] array2){

    return ArrayUtils.addAll(array1, array2);
  }
}
