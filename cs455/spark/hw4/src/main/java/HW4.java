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

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;


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

	Dataset data = dataFull.select("artist_terms", "danceability", "duration", "end_of_fade_in",
			 "energy", "key", "loudness", "mode", "start_of_fade_out", "tempo",
				       "time_signature", "year").as(Encoders.bean(Song.class));

       	//Dataset data = getFirstTerms(dataSub, "artist_terms", DataTypes.StringType).as(Encoders.bean(Song.class));

	data.printSchema();

	
	StructType libsvmSchema = new StructType().add("label", "double").add("features", new VectorUDT());  

	Dataset dsLibsvm = spark.createDataFrame(
						 data.javaRDD().map(new Function<Song, Row>() {  
						      public Row call(Song s) {							  
							  double label = (double) s.getYear();
							  double[] features = {s.getDanceability(), s.getDuration(), s.getEnd_Of_Fade_In(), s.getEnergy(), s.getKey(), s.getLoudness(), s.getMode(),
									       s.getStart_Of_Fade_Out(), s.getTempo(), s.getTime_Signature(), s.getYear()};
							  Vector currentRow = Vectors.dense(features);  
							  return RowFactory.create(label, currentRow);  
						      }  
						  }), libsvmSchema);   

	
	// Index labels, adding metadata to the label column.
	// Fit on whole dataset to include all labels in index.
	StringIndexerModel labelIndexer = new StringIndexer()
	    .setInputCol("label")
	    .setOutputCol("indexedLabel")
	    .fit(dsLibsvm);

	// Automatically identify categorical features, and index them.
	VectorIndexerModel featureIndexer = new VectorIndexer()
	    .setInputCol("features")
	    .setOutputCol("indexedFeatures")
	    .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
	    .fit(dsLibsvm);

	// Split the data into training and test sets (30% held out for testing).
	Dataset<Row>[] splits = dsLibsvm.randomSplit(new double[]{0.7, 0.3});
	Dataset<Row> trainingData = splits[0];
	Dataset<Row> testData = splits[1];

	// Train a DecisionTree model.
	DecisionTreeClassifier dt = new DecisionTreeClassifier()
	    .setLabelCol("indexedLabel")
	    .setFeaturesCol("indexedFeatures");

	// Convert indexed labels back to original labels.
	IndexToString labelConverter = new IndexToString()
	    .setInputCol("prediction")
	    .setOutputCol("predictedLabel")
	    .setLabels(labelIndexer.labels());

	// Chain indexers and tree in a Pipeline.
	Pipeline pipeline = new Pipeline()
	    .setStages(new PipelineStage[]{labelIndexer, featureIndexer, dt, labelConverter});

	// Train model. This also runs the indexers.
	PipelineModel model = pipeline.fit(trainingData);

	// Make predictions.
	Dataset<Row> predictions = model.transform(testData);

	// Select example rows to display.
	predictions.select("predictedLabel", "label", "features").show(5);

	predictions.select("predictedLabel", "label", "features").write().format("json").save("/HW4_output/test/classification");
	
	// Select (prediction, true label) and compute test error.
	MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
	    .setLabelCol("indexedLabel")
	    .setPredictionCol("prediction")
	    .setMetricName("accuracy");
	double accuracy = evaluator.evaluate(predictions);
	System.out.println("Test Error = " + (1.0 - accuracy));

	DecisionTreeClassificationModel treeModel =
	    (DecisionTreeClassificationModel) (model.stages()[2]);
	System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
	
	/*
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
