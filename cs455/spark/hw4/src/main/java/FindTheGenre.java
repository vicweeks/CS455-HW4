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

public class FindTheGenre implements Serializable {

  private final String[] doubleArraysInData = {"bars_start", "beats_start", "segments_loudness_max",
      "segments_pitches", "tatums_start", "segments_timbre", "segments_start", "sections_start"};

  private final Encoder<Tuple2<String, Double>> encoderForPrintingCounts = Encoders
      .tuple(Encoders.STRING(), Encoders.DOUBLE());


  private final Dataset<Row> dataFull;

  public FindTheGenre(Dataset<Row> dataFull) {
    this.dataFull = dataFull;
  }

  public void run() {
    //Machine learning

    Dataset artistFirstTerm = RowParser
        .getFirstTerms(dataFull, "artist_terms", DataTypes.StringType);

    Dataset dataset = RowParser.makeDoubleArrays(artistFirstTerm, doubleArraysInData);
    //dataset.printSchema();

    Dataset data = dataset.as(Encoders.bean(Song.class));

    StructType libsvmSchema = new StructType().add("label", "String")
        .add("features", new VectorUDT());

    Dataset dsLibsvm = data.sparkSession().createDataFrame(
        data.javaRDD().map(new Function<Song, Row>() {
          public Row call(Song s) {
            String label = s.getArtist_Terms();

            Vector currentRow = Vectors.dense(s.getFeatures());
            return RowFactory.create(label, currentRow);
          }
        }), libsvmSchema);

    dsLibsvm = dsLibsvm.filter(col("label").isNotNull());

    // Save info on the data we are working on
    double startCount = (double) dataset.select(col("artist_terms")).count();
    double endCount =(double) dsLibsvm.select(col("label")).count();

    System.out.println("Songs before filter: " + startCount);
    System.out.println("Songs after filter: " + endCount);

    List<Tuple2<String, Double>> songCountsInfo = Arrays
        .asList(new Tuple2("Songs before filter: ", startCount),
            new Tuple2("Songs after filter: ", endCount));
    Dataset totalCountsOfGenres = dsLibsvm.groupBy(col("label")).count();
    Dataset sortedGenres = totalCountsOfGenres.coalesce(1).orderBy(col("count").desc());

    sortedGenres.persist();

    sortedGenres.write().mode(SaveMode.Overwrite).format("json").save("/HW4/Classification/DataUsedInfo/genreCounts");
    sortedGenres.show(10);
    saveTestResults(songCountsInfo, sortedGenres, "/HW4/Classification/DataUsedInfo/SongCounts");

    sortedGenres.unpersist();

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    StringIndexerModel labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(dsLibsvm);

    // Split the data into training and test sets (30% held out for testing).
    Dataset<Row>[] splits = dsLibsvm.randomSplit(new double[]{0.7, 0.3});
    Dataset<Row> trainingData = splits[0];
    Dataset<Row> testData = splits[1];

    // Convert indexed labels back to original labels.
    IndexToString labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels());

    // Select (prediction, true label) and compute test error.
    MulticlassClassificationEvaluator evaluatorAcc = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy");

    MulticlassClassificationEvaluator evaluatorPrec = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("weightedPrecision");

    MulticlassClassificationEvaluator evaluatorRecall = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("weightedRecall");

    decisionTreeClassifier(trainingData, testData, labelIndexer, labelConverter, evaluatorAcc,
        evaluatorPrec, evaluatorRecall);
    randomForestClassifier(trainingData, testData, labelIndexer, labelConverter, evaluatorAcc,
        evaluatorPrec, evaluatorRecall);
    logisticRegressionClassifier(trainingData, testData, labelIndexer, labelConverter, evaluatorAcc,
        evaluatorPrec, evaluatorRecall);

  }

  private void decisionTreeClassifier(Dataset trainingData, Dataset testData,
      StringIndexerModel labelIndexer,
      IndexToString labelConverter,
      MulticlassClassificationEvaluator evaluatorAcc,
      MulticlassClassificationEvaluator evaluatorPrec,
      MulticlassClassificationEvaluator evaluatorRecall) {

    DecisionTreeClassifier dt = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features");

    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{labelIndexer, dt, labelConverter});

    // Train model. This also runs the indexers.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    Dataset<Row> trainingFit = model.transform(trainingData);
    Dataset<Row> predictions = model.transform(testData);

    trainingFit.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/DecisionTree/train");

    predictions.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/DecisionTree/test");

    double trainAcc = evaluatorAcc.evaluate(trainingFit);
    double testAcc = evaluatorAcc.evaluate(predictions);
    System.out.println();
    System.out.println("Decision Tree Train Accuracy = " + trainAcc);
    System.out.println("Decision Tree Test  Accuracy = " + testAcc);

    double trainPrec = evaluatorPrec.evaluate(trainingFit);
    double testPrec = evaluatorPrec.evaluate(predictions);
    System.out.println("Decision Tree Train Weighted Predictions = " + trainPrec);
    System.out.println("Decision Tree Test  Weighted Predictions= " + testPrec);

    double trainRecall = evaluatorRecall.evaluate(trainingFit);
    double testRecall = evaluatorRecall.evaluate(predictions);
    System.out.println("Decision Tree Train Weighted Recall = " + trainRecall);
    System.out.println("Decision Tree Test  Weighted Recall = " + testRecall);

    List<Tuple2<String, Double>> results = Arrays.asList(
        new Tuple2("Decision Tree Train Accuracy = ", trainAcc),
        new Tuple2("Decision Tree Test  Accuracy = ", testAcc),
        new Tuple2("Decision Tree Train Weighted Predictions = ", trainPrec),
        new Tuple2("Decision Tree Test  Weighted Predictions= ", testPrec),
        new Tuple2("Decision Tree Train Weighted Recall = ", trainRecall),
        new Tuple2("Decision Tree Test  Weighted Recall = ", testRecall));

    saveTestResults(results, predictions, "/HW4/Classification/DecisionTree/results");
  }

  private void randomForestClassifier(Dataset trainingData, Dataset testData,
      StringIndexerModel labelIndexer,
      IndexToString labelConverter,
      MulticlassClassificationEvaluator evaluatorAcc,
      MulticlassClassificationEvaluator evaluatorPrec,
      MulticlassClassificationEvaluator evaluatorRecall) {

    RandomForestClassifier rf = new RandomForestClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features");

    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{labelIndexer, rf, labelConverter});

    // Train model. This also runs the indexers.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    Dataset<Row> trainingFit = model.transform(trainingData);
    Dataset<Row> predictions = model.transform(testData);

    trainingFit.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/RandomForest/train");

    predictions.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/RandomForest/test");

    double trainAcc = evaluatorAcc.evaluate(trainingFit);
    double testAcc = evaluatorAcc.evaluate(predictions);
    System.out.println();
    System.out.println("Random Forest Train Accuracy = " + trainAcc);
    System.out.println("Random Forest Test  Accuracy = " + testAcc);

    double trainPrec = evaluatorPrec.evaluate(trainingFit);
    double testPrec = evaluatorPrec.evaluate(predictions);
    System.out.println("Random Forest Train Weighted Predictions = " + trainPrec);
    System.out.println("Random Forest Test  Weighted Predictions= " + testPrec);

    double trainRecall = evaluatorRecall.evaluate(trainingFit);
    double testRecall = evaluatorRecall.evaluate(predictions);
    System.out.println("Random Forest Train Weighted Recall = " + trainRecall);
    System.out.println("Random Forest Test  Weighted Recall = " + testRecall);

    List<Tuple2<String, Double>> results = Arrays.asList(
        new Tuple2("Random Forest Train Accuracy = ", trainAcc),
        new Tuple2("Random Forest Test  Accuracy = ", testAcc),
        new Tuple2("Random Forest Train Weighted Predictions = ", trainPrec),
        new Tuple2("Random Forest Test  Weighted Predictions= ", testPrec),
        new Tuple2("Random Forest Train Weighted Recall = ", trainRecall),
        new Tuple2("Random Forest Test  Weighted Recall = ", testRecall));

    saveTestResults(results, predictions, "/HW4/Classification/RandomForest/results");

  }

  private void logisticRegressionClassifier(Dataset trainingData, Dataset testData,
      StringIndexerModel labelIndexer,
      IndexToString labelConverter,
      MulticlassClassificationEvaluator evaluatorAcc,
      MulticlassClassificationEvaluator evaluatorPrec,
      MulticlassClassificationEvaluator evaluatorRecall) {

    // create the trainer and set its parameters
    LogisticRegression lr = new LogisticRegression()
        .setLabelCol("indexedLabel")
        .setMaxIter(100)
        .setRegParam(0.3)
        .setElasticNetParam(0.8)
        .setFamily("multinomial");

    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{labelIndexer, lr, labelConverter});

    // Train model. This also runs the indexers.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    Dataset<Row> trainingFit = model.transform(trainingData);
    Dataset<Row> predictions = model.transform(testData);

    trainingFit.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/Regression/train");

    predictions.select("predictedLabel", "label")
        .coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save("/HW4/Classification/Regression/test");

    double trainAcc = evaluatorAcc.evaluate(trainingFit);
    double testAcc = evaluatorAcc.evaluate(predictions);
    System.out.println();
    System.out.println("Logistic Regression Train Accuracy = " + trainAcc);
    System.out.println("Logistic Regression Test  Accuracy = " + testAcc);

    double trainPrec = evaluatorPrec.evaluate(trainingFit);
    double testPrec = evaluatorPrec.evaluate(predictions);
    System.out.println("Logistic Regression Train Weighted Predictions = " + trainPrec);
    System.out.println("Logistic Regression Test  Weighted Predictions= " + testPrec);

    double trainRecall = evaluatorRecall.evaluate(trainingFit);
    double testRecall = evaluatorRecall.evaluate(predictions);
    System.out.println("Logistic Regression Train Weighted Recall = " + trainRecall);
    System.out.println("Logistic Regression Test  Weighted Recall = " + testRecall);

    List<Tuple2<String, Double>> results = Arrays.asList(
        new Tuple2("Logistic Regression Train Accuracy = ", trainAcc),
        new Tuple2("Logistic Regression Test  Accuracy = ", testAcc),
        new Tuple2("Logistic Regression Train Weighted Predictions = ", trainPrec),
        new Tuple2("Logistic Regression Test  Weighted Predictions= ", testPrec),
        new Tuple2("Logistic Regression Train Weighted Recall = ", trainRecall),
        new Tuple2("Logistic Regression Test  Weighted Recall = ", testRecall));

    saveTestResults(results, predictions, "/HW4/Classification/Regression/results");

  }

  private void saveTestResults(List<Tuple2<String, Double>> results, Dataset dataset,
      String saveLocation) {
    Dataset writer = dataset.sparkSession()
        .createDataset(results, encoderForPrintingCounts);

    writer.coalesce(1).write().mode(SaveMode.Overwrite).format("json")
        .save(saveLocation);
  }

}
