/*
 * CS455: HW4 Term Project
 * Authors: Victor Weeks, Diego Batres, Josiah May
 */

import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.util.regex.Pattern;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.substring_index;

// TODO

import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.Vector;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.api.java.JavaRDD;

public final class HW4 {
    private static final Pattern TAB = Pattern.compile("\t");

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

	Dataset<Row> data = spark.read().format("csv")
	    .option("sep", "\t")
	    .option("inferSchema", "true")
	    .option("header", "true")
	    .load(dataLoc);

	Dataset<Row> condData = data.select("artist_familiarity", "artist_hotttnesss", "artist_name",
					    "artist_terms", "segments_timbre", "year");

	condData.printSchema();
	
	JavaRDD<Row> condDataRDD = condData.javaRDD();
	
	StructType schema = new StructType(new StructField[]{
		new StructField("artist_familiarity", DataTypes.DoubleType, true, Metadata.empty()),
		new StructField("artist_hotttnesss", DataTypes.DoubleType, true, Metadata.empty()),
		new StructField("artist_name", DataTypes.StringType, true, Metadata.empty()),
		new StructField("artist_terms", DataTypes.StringType, true, Metadata.empty()),
		new StructField("segments_timbre", DataTypes.StringType, true, Metadata.empty()),
		new StructField("year", DataTypes.IntegerType, true, Metadata.empty())
	    });

	Dataset<Row> screenedData1 = spark.createDataFrame(condDataRDD, schema);
	Dataset<Row> screenedData2 = getSplitTerms(screenedData1, "artist_terms", DataTypes.StringType);
 	Dataset<Row> screenedData = getSplitTerms(screenedData2, "segments_timbre", DataTypes.DoubleType);       
	
	//screenedData.printSchema();
	screenedData.select("segments_timbre").show();
	//screenedData.select("segments_timbre").write().format("json").save("/HW4_output/condTest");
	
	//Dataset<Row> splitTerms = getFirstTerms(data, "artist_terms", DataTypes.StringType );

	//	Dataset<Row> splitTerms2 = getSplitTerms(splitTerms, "artist_terms_freq", DataTypes.DoubleType );

	//	Dataset<Row> termsGroup = splitTerms.groupBy("artist_terms").avg("tempo");

	//	termsGroup.show();
	/*
	Word2Vec word2Vec = new Word2Vec()
	    .setInputCol("artist_terms")
	    .setOutputCol("terms_vec");
	    
	Word2VecModel model = word2Vec.fit(splitTerms);
	Dataset<Row> result = model.transform(splitTerms);
	*/
	//result.printSchema();
	//result.select("terms_vec").write().format("json").save("/HW4_output/");
	//splitTerms2.printSchema(); // show how data is saved

	//splitTerms2.select("artist_terms", "artist_terms_freq").show(5); // print out the term being tested
	
	//splitTerms2.select("artist_terms", "artist_terms_freq").write().format("json").save("/home/HW4/Example/terms_test"); // write to json

  // Test machine learning models
  /*
  FPGrowthModel model = new FPGrowth()
      .setItemsCol("artist_terms")
      .setMinSupport(0.5)
      .setMinConfidence(0.6)
      .fit(splitTerms);


      // Display frequent itemsets.
  model.freqItemsets().show();
      // Display generated association rules.
  model.associationRules().show();
  */


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
