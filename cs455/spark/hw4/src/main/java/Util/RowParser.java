package Util;

import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.substring_index;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Helper methods for manipulating information in data sets
 */
public class RowParser {

  /**
   * Splits a string into an array and removes all chars that were part of the array setup ( " \ [ ] )
   * and saves it to a new column
   * It changes the array type to the values given
   * @param data Dataset to read
   * @param columnName column to change
   * @param dataType the type of the array
   * @return The new dataset with the changed info
   */
  public static Dataset<Row> getSplitTerms(Dataset<Row> data, String columnName, DataType dataType) {
    return data.withColumn(columnName ,split(
        regexp_replace(data.col(columnName), "[\\\\\"\\[\\]]+", ""), ", ").cast(DataTypes.createArrayType( dataType)));
  }


  /**
   * Gets the first element of a string[] and returns an array of just that element and saves it to a new column
   * It need to be an array for the Machine learning models
   * @param data Dataset to read
   * @param columnName column to change
   * @param dataType the type of the array
   * @return The new dataset with the changed info
   */
  public static Dataset<Row> getFirstTerms(Dataset<Row> data, String columnName, DataType dataType) {
    return getFirstNterms(data, columnName, dataType, 1);
  }

  /**
   * Gets the first element of a string[] and returns an array of just that element and saves it to a new column
   * It need to be an array for the Machine learning models
   * @param data Dataset to read
   * @param columnName column to change
   * @param dataType the type of the array
   * @return The new dataset with the changed info
   */
  public static Dataset<Row> getFirstNterms(Dataset<Row> data, String columnName, DataType dataType, int numberOfItems) {
    return data.withColumn(columnName ,substring_index(
        regexp_replace(data.col(columnName), "[\\\\\"\\[\\]]+", ""), ", ", numberOfItems).cast( dataType));
  }


  /**
   * Combines two double arrays into one double array
   * @param array1 first array
   * @param array2 second array
   * @return combined array
   */
  public static double[] combineDoubles(double[] array1, double[] array2){
    return ArrayUtils.addAll(array1, array2);
  }


  /**
   * Takes all the data and splits all the strings that have double arrays into double arrays
   * @param data the music data
   * @param columns what terms you are splitting
   * @return the data with split columns
   */
  public static Dataset<Row> makeDoubleArrays(Dataset<Row> data, String[] columns){
    Dataset<Row> rt =  data;

    for (int i = 0; i < columns.length; i++) {
      rt = RowParser.getSplitTerms(rt, columns[i], DataTypes.DoubleType);
    }
    return rt;
  }

}
