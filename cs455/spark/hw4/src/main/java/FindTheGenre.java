import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import Util.RowParser;
import java.io.Serializable;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
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
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.stat.Correlation;

public class FindTheGenre  implements Serializable {

  private final String[] doubleArraysInData = {"bars_start", "beats_start", "segments_loudness_max",
      "segments_pitches", "tatums_start", "segments_timbre", "segments_start", "sections_start"};

  private final Dataset<Row> dataFull;

  public FindTheGenre(Dataset<Row> dataFull) {
    this.dataFull = dataFull;
  }

  public void run(){
    //Machine learning

    Dataset dataFixed7 = RowParser
        .getFirstNterms(dataFull, "artist_terms", "artist_terms", DataTypes.StringType, 5);
    Dataset dataFixed6 = RowParser.getSplitTerms(dataFixed7, "artist_terms", "artist_terms", DataTypes.StringType);
    Dataset dataFixed5 = dataFixed6.withColumn("artist_terms", explode(col("artist_terms")));


    Dataset dataset = RowParser.makeDoubleArrays(dataFixed5, doubleArraysInData);
/*
    Dataset dataFixed4 = RowParser.getFirstNterms(dataFixed5, "segments_timbre", "segments_timbre", DataTypes.StringType, 1872);
    Dataset dataFixed3 = RowParser.getSplitTerms(dataFixed4, "segments_timbre", "segments_timbre", DataTypes.DoubleType);
    Dataset dataFixed2 = RowParser.getFirstNterms(dataFixed3, "segments_start", "segments_start", DataTypes.StringType, 312);
    Dataset dataFixed1 = RowParser.getSplitTerms(dataFixed2, "segments_start", "segments_start", DataTypes.DoubleType);
*/


    Dataset data = dataset.select("artist_terms", "danceability", "duration", "end_of_fade_in",
        "energy", "key", "loudness", "mode", "start_of_fade_out", "tempo",
        "time_signature", "year", "segments_start", "segments_timbre", "tatums_start", "bars_start",
        "beats_start", "segments_loudness_max", "segments_pitches", "sections_start").as(Encoders.bean(Song.class));


    StructType libsvmSchema = new StructType().add("label", "String").add("features", new VectorUDT());


    Dataset dsLibsvm = data.sparkSession().createDataFrame(
        data.javaRDD().map(new Function<Song, Row>() {
          public Row call(Song s) {
            String label =  s.getArtist_Terms();

            Vector currentRow = Vectors.dense(s.getFeatures());
            return RowFactory.create(label, currentRow);
          }
        }), libsvmSchema);


    dsLibsvm.write().mode(SaveMode.Overwrite).format("json").save("/HW4_output/libsvm");
    
    Row r1 = Correlation.corr(dsLibsvm, "features").head();
    System.out.println("Pearson correlation matrix:\n" + r1.get(0).toString());
    
	
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    StringIndexerModel labelIndexer = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indexedLabel")
        .fit(dsLibsvm);

    /*
    // Automatically identify categorical features, and index them.
    VectorIndexerModel featureIndexer = new VectorIndexer()
        .setInputCol("features")
        .setOutputCol("indexedFeatures")
        .setMaxCategories(4) // features with > 4 distinct values are treated as continuous.
        .fit(dsLibsvm);
    */
    
    // Split the data into training and test sets (30% held out for testing).
    Dataset<Row>[] splits = dsLibsvm.randomSplit(new double[]{0.7, 0.3});
    Dataset<Row> trainingData = splits[0];
    Dataset<Row> testData = splits[1];

    // Train a DecisionTree model.
    RandomForestClassifier rf = new RandomForestClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features");

    // Convert indexed labels back to original labels.
    IndexToString labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels());

    // Chain indexers and tree in a Pipeline.
    Pipeline pipeline = new Pipeline()
        .setStages(new PipelineStage[]{labelIndexer, rf, labelConverter});

    // Train model. This also runs the indexers.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    Dataset<Row> trainingFit = model.transform(trainingData);
    Dataset<Row> predictions = model.transform(testData);

    // Select example rows to display.
    trainingFit.select("predictedLabel", "label", "features").show(5);
    predictions.select("predictedLabel", "label", "features").show(5);

    //predictions.select("predictedLabel", "label").coalesce(1).write().mode(SaveMode.Overwrite).format("json").save("/home/HW4_output/test/classification");

    // Select (prediction, true label) and compute test error.
    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("indexedLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy");

    double trainingAcc = evaluator.evaluate(trainingFit);
    double accuracy = evaluator.evaluate(predictions);
    System.out.println("Train Error = " + (1.0 - trainingAcc));
    System.out.println("Test  Error = " + (1.0 - accuracy));

    /*
    DecisionTreeClassificationModel treeModel =
        (DecisionTreeClassificationModel) (model.stages()[2]);
    System.out.println("Learned classification tree model:\n" + treeModel.toDebugString());
    */
  }
    
  private Dataset<Row> makeDoubleArrays(Dataset<Row> data){

    Dataset<Row> rt =  data;

    for (int i = 0; i < doubleArraysInData.length; i++) {
      rt = RowParser.getSplitTerms(rt, doubleArraysInData[i], DataTypes.DoubleType);
    }
    return rt;
  }

}
