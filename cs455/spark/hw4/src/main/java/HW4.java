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

	Dataset<Row> splitTerms = getSplitTerms(data, "artist_terms", DataTypes.StringType );

	Dataset<Row> splitTerms2 = getSplitTerms(splitTerms, "artist_terms_freq", DataTypes.DoubleType );

	splitTerms2.printSchema();

	splitTerms2.select("artist_terms", "artist_terms_freq").write().format("json").save("/home/HW4/Example/terms_test");

	
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
		return data.withColumn(columnName ,split(
        regexp_replace(data.col(columnName), "[\\\\\"\\[\\]]+", ""), ", ").cast(DataTypes.createArrayType( dataType)));
	}
}
