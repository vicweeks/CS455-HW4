/*
 * CS455: HW4 Term Project
 * Authors: Victor Weeks, Diego Batres, Josiah May
 */

import org.apache.spark.sql.SparkSession;

import org.apache.spark.ml.feature.RegexTokenizer;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.trim;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StringType;
// TODO

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

	Dataset<Row> splitTerms = data.withColumn("artist_terms",split(
      regexp_replace(data.col("artist_terms"), "[\\\\\"\\[\\]]+", ""), ", ").cast(DataTypes.createArrayType(DataTypes.StringType)));
	splitTerms.printSchema();



  splitTerms.select("artist_terms").write().format("json").save("/home/HW4/Example/terms_test");

	
	spark.stop();
    }
}
