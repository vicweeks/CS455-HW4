import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import Util.RowParser;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassificationModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.stat.Correlation;
import scala.Tuple2;

/**
 * Pre-processes and analyzes data
 */
public class FindTheGenre implements Serializable {

  // Array of data fields
  private final String[] doubleArraysInData = {"bars_start", "beats_start", "segments_loudness_max",
      "segments_pitches", "tatums_start", "segments_timbre", "segments_start", "sections_start"};

  // Encoder for printing counts
  private final Encoder<Tuple2<String, Double>> encoderForPrintingCounts = Encoders
      .tuple(Encoders.STRING(), Encoders.DOUBLE());

  // Dataset used for processing
  private final Dataset<Row> dataFull;

  /**
   * Sets up dataset for analysis
   * @param dataFull the dataset to analyse
   */
  public FindTheGenre(Dataset<Row> dataFull) {
    this.dataFull = dataFull;
  }

  /**
   * Pre-process and analyze data
   */
  public void run() {

    // Get first artist term
    Dataset artistFirstTerm = RowParser
        .getFirstTerms(dataFull, "artist_terms", DataTypes.StringType);

    // Takes all array strings and turns them into arrays
    Dataset data = RowParser.makeDoubleArrays(artistFirstTerm, doubleArraysInData).as(Encoders.bean(Song.class));

    // Sets up dataset for inputting into regression models
    StructType libsvmSchema = new StructType().add("label", "String")
        .add("features", new VectorUDT());

    // Creates data frame for all features
    Dataset dsLibsvm = data.sparkSession().createDataFrame(
        data.javaRDD().map(new Function<Song, Row>() {
          public Row call(Song s) {
            String label = s.getArtist_Terms();
            Vector currentRow = Vectors.dense(s.getFeatures());
            return RowFactory.create(label, currentRow);
          }
        }), libsvmSchema);

    // Remove null entries
    dsLibsvm = dsLibsvm.filter(col("label").isNotNull());

    // Save info on the data we are working on
    double startCount = (double) data.select(col("artist_terms")).count();
    double endCount =(double) dsLibsvm.select(col("label")).count();

    // Print number of songs before and after filtering
    System.out.println("Songs before filter: " + startCount);
    System.out.println("Songs after filter: " + endCount);

    // Sort genres by count
    List<Tuple2<String, Double>> songCountsInfo = Arrays
        .asList(new Tuple2("Songs before filter: ", startCount),
            new Tuple2("Songs after filter: ", endCount));
    Dataset totalCountsOfGenres = dsLibsvm.select("label").groupBy(col("label")).count();
    Dataset sortedGenres = totalCountsOfGenres.coalesce(1).orderBy(col("count").desc());

    // Persist sorted genre dataset
    sortedGenres.persist();

    // Save genre and song count datasets to output files
    sortedGenres.write().mode(SaveMode.Overwrite).format("json").save("/HW4/Classification/DataUsedInfo/genreCounts");
    sortedGenres.show(10);
    saveTestResults(songCountsInfo, sortedGenres, "/HW4/Classification/DataUsedInfo/SongCounts");

    // Unpersist sorted genre dataset
    sortedGenres.unpersist();

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    StringIndexerModel labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(dsLibsvm);

    // Split the data into training and test sets (30% held out for testing).
    Dataset<Row>[] splits = dsLibsvm.randomSplit(new double[]{0.7, 0.3});
    Dataset<Row> trainingData = splits[0].cache();
    Dataset<Row> testData = splits[1].cache();

    // Convert indexed labels back to original labels.
    IndexToString labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels());

    // Select (prediction, true label) and compute test error for accuracy
    MulticlassClassificationEvaluator evaluatorAcc = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy");

    // Select (prediction, true label) and compute test error for weighted precision
    MulticlassClassificationEvaluator evaluatorPrec = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("weightedPrecision");

    // Select (prediction, true label) and compute test error for weighted recall
    MulticlassClassificationEvaluator evaluatorRecall = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("weightedRecall");

    // Run analysis using three different machine learning algorithms:
    // Decision tree classifier, random forest classifier, logistic regression classifier
    decisionTreeClassifier(trainingData, testData, labelIndexer, labelConverter, evaluatorAcc,
        evaluatorPrec, evaluatorRecall);
    randomForestClassifier(trainingData, testData, labelIndexer, labelConverter, evaluatorAcc,
        evaluatorPrec, evaluatorRecall);
    logisticRegressionClassifier(trainingData, testData, labelIndexer, labelConverter, evaluatorAcc,
        evaluatorPrec, evaluatorRecall);
  }

  /**
   * Performs analysis on genres using decision tree classifier
   * @param trainingData data ued for training
   * @param testData data used for testing
   * @param labelIndexer changes labels to integer format
   * @param labelConverter changes labels back to strings
   * @param evaluatorAcc evaluates accuracy
   * @param evaluatorPrec evaluates weighted precision
   * @param evaluatorRecall evaluates weighted recall
   */
  private void decisionTreeClassifier(Dataset trainingData, Dataset testData,
      StringIndexerModel labelIndexer,
      IndexToString labelConverter,
      MulticlassClassificationEvaluator evaluatorAcc,
      MulticlassClassificationEvaluator evaluatorPrec,
      MulticlassClassificationEvaluator evaluatorRecall) {

    // Create the trainer and set its parameters.
    DecisionTreeClassifier dt = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features");

    // Create pipeline to apply in sequence.
    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{labelIndexer, dt, labelConverter});

    // Train model. This also runs the indexers.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    Dataset<Row> trainingFit = model.transform(trainingData);
    Dataset<Row> predictions = model.transform(testData);

    // Print table of predicted and actual labels
    trainingFit.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/DecisionTree/train");
    predictions.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/DecisionTree/test");

    // Print training and testing accuracy
    double trainAcc = evaluatorAcc.evaluate(trainingFit);
    double testAcc = evaluatorAcc.evaluate(predictions);
    System.out.println();
    System.out.println("Decision Tree Train Accuracy = " + trainAcc);
    System.out.println("Decision Tree Test  Accuracy = " + testAcc);

    // Print training and testing weighted precision
    double trainPrec = evaluatorPrec.evaluate(trainingFit);
    double testPrec = evaluatorPrec.evaluate(predictions);
    System.out.println("Decision Tree Train Weighted Predictions = " + trainPrec);
    System.out.println("Decision Tree Test  Weighted Predictions= " + testPrec);

    // Print training and testing weighted recall
    double trainRecall = evaluatorRecall.evaluate(trainingFit);
    double testRecall = evaluatorRecall.evaluate(predictions);
    System.out.println("Decision Tree Train Weighted Recall = " + trainRecall);
    System.out.println("Decision Tree Test  Weighted Recall = " + testRecall);

    // Print final results
    List<Tuple2<String, Double>> results = Arrays.asList(
        new Tuple2("Decision Tree Train Accuracy = ", trainAcc),
        new Tuple2("Decision Tree Test  Accuracy = ", testAcc),
        new Tuple2("Decision Tree Train Weighted Predictions = ", trainPrec),
        new Tuple2("Decision Tree Test  Weighted Predictions= ", testPrec),
        new Tuple2("Decision Tree Train Weighted Recall = ", trainRecall),
        new Tuple2("Decision Tree Test  Weighted Recall = ", testRecall));

    // Save results to output file
    saveTestResults(results, predictions, "/HW4/Classification/DecisionTree/results");
  }

  /**
   * Performs analysis on genres using random forest classifier
   * @param trainingData data ued for training
   * @param testData data used for testing
   * @param labelIndexer changes labels to integer format
   * @param labelConverter changes labels back to strings
   * @param evaluatorAcc evaluates accuracy
   * @param evaluatorPrec evaluates weighted precision
   * @param evaluatorRecall evaluates weighted recall
   */
  private void randomForestClassifier(Dataset trainingData, Dataset testData,
      StringIndexerModel labelIndexer,
      IndexToString labelConverter,
      MulticlassClassificationEvaluator evaluatorAcc,
      MulticlassClassificationEvaluator evaluatorPrec,
      MulticlassClassificationEvaluator evaluatorRecall) {

    // Create the trainer and set its parameters.
    RandomForestClassifier rf = new RandomForestClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features");

    // Create pipeline to apply in sequence.
    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{labelIndexer, rf, labelConverter});

    // Train model. This also runs the indexers.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    Dataset<Row> trainingFit = model.transform(trainingData);
    Dataset<Row> predictions = model.transform(testData);

    // Print table of predicted and actual labels
    trainingFit.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/RandomForest/train");
    predictions.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/RandomForest/test");

    // Print training and testing accuracy
    double trainAcc = evaluatorAcc.evaluate(trainingFit);
    double testAcc = evaluatorAcc.evaluate(predictions);
    System.out.println();
    System.out.println("Random Forest Train Accuracy = " + trainAcc);
    System.out.println("Random Forest Test  Accuracy = " + testAcc);

    // Print training and testing weighted precision
    double trainPrec = evaluatorPrec.evaluate(trainingFit);
    double testPrec = evaluatorPrec.evaluate(predictions);
    System.out.println("Random Forest Train Weighted Predictions = " + trainPrec);
    System.out.println("Random Forest Test  Weighted Predictions= " + testPrec);

    // Print training and testing weighted recall
    double trainRecall = evaluatorRecall.evaluate(trainingFit);
    double testRecall = evaluatorRecall.evaluate(predictions);
    System.out.println("Random Forest Train Weighted Recall = " + trainRecall);
    System.out.println("Random Forest Test  Weighted Recall = " + testRecall);

    // Print final results
    List<Tuple2<String, Double>> results = Arrays.asList(
        new Tuple2("Random Forest Train Accuracy = ", trainAcc),
        new Tuple2("Random Forest Test  Accuracy = ", testAcc),
        new Tuple2("Random Forest Train Weighted Predictions = ", trainPrec),
        new Tuple2("Random Forest Test  Weighted Predictions= ", testPrec),
        new Tuple2("Random Forest Train Weighted Recall = ", trainRecall),
        new Tuple2("Random Forest Test  Weighted Recall = ", testRecall));

    // Save results to output file
    saveTestResults(results, predictions, "/HW4/Classification/RandomForest/results");
  }

  /**
   * Performs analysis on genres using logistic regression
   * @param trainingData data ued for training
   * @param testData data used for testing
   * @param labelIndexer changes labels to integer format
   * @param labelConverter changes labels back to strings
   * @param evaluatorAcc evaluates accuracy
   * @param evaluatorPrec evaluates weighted precision
   * @param evaluatorRecall evaluates weighted recall
   */
  private void logisticRegressionClassifier(Dataset trainingData, Dataset testData,
      StringIndexerModel labelIndexer,
      IndexToString labelConverter,
      MulticlassClassificationEvaluator evaluatorAcc,
      MulticlassClassificationEvaluator evaluatorPrec,
      MulticlassClassificationEvaluator evaluatorRecall) {

    // Create the trainer and set its parameters.
    LogisticRegression lr = new LogisticRegression()
        .setLabelCol("indexedLabel")
        .setMaxIter(100)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)
        .setFamily("multinomial");

    // Create pipeline to apply in sequence.
    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{labelIndexer, lr, labelConverter});

    // Train model. This also runs the indexers.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    Dataset<Row> trainingFit = model.transform(trainingData);
    Dataset<Row> predictions = model.transform(testData);

    // Print table of predicted and actual labels
    trainingFit.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/Regression/train");
    predictions.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/Regression/test");

    // Print training and testing accuracy
    double trainAcc = evaluatorAcc.evaluate(trainingFit);
    double testAcc = evaluatorAcc.evaluate(predictions);
    System.out.println();
    System.out.println("Logistic Regression Train Accuracy = " + trainAcc);
    System.out.println("Logistic Regression Test  Accuracy = " + testAcc);

    // Print training and testing weighted precision
    double trainPrec = evaluatorPrec.evaluate(trainingFit);
    double testPrec = evaluatorPrec.evaluate(predictions);
    System.out.println("Logistic Regression Train Weighted Predictions = " + trainPrec);
    System.out.println("Logistic Regression Test  Weighted Predictions= " + testPrec);

    // Print training and testing weighted recall
    double trainRecall = evaluatorRecall.evaluate(trainingFit);
    double testRecall = evaluatorRecall.evaluate(predictions);
    System.out.println("Logistic Regression Train Weighted Recall = " + trainRecall);
    System.out.println("Logistic Regression Test  Weighted Recall = " + testRecall);

    // Print final results
    List<Tuple2<String, Double>> results = Arrays.asList(
        new Tuple2("Logistic Regression Train Accuracy = ", trainAcc),
        new Tuple2("Logistic Regression Test  Accuracy = ", testAcc),
        new Tuple2("Logistic Regression Train Weighted Predictions = ", trainPrec),
        new Tuple2("Logistic Regression Test  Weighted Predictions= ", testPrec),
        new Tuple2("Logistic Regression Train Weighted Recall = ", trainRecall),
        new Tuple2("Logistic Regression Test  Weighted Recall = ", testRecall));

    // Save results to output file
    saveTestResults(results, predictions, "/HW4/Classification/Regression/results");
  }

  /**
   * Saves test results in HDFS as an output file
   * @param results list of items to write
   * @param dataset data set
   * @param saveLocation location to be saved in
   */
  private void saveTestResults(List<Tuple2<String, Double>> results, Dataset dataset,
      String saveLocation) {
    Dataset writer = dataset.sparkSession()
        .createDataset(results, encoderForPrintingCounts);
    writer.coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save(saveLocation);
  }

}
