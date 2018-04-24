package Util;

import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.substring_index;

import java.util.InvalidPropertiesFormatException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class RowParser {

  public static final String[] doubleArraysInData = {"bars_start", "beats_start", "segments_loudness_max",
      "segments_pitches", "tatums_start", "segments_timbre", "segments_start"};
  public static final int[] maxCountForData = { 32,130,312,1872,340,312,312};

  /**
   * Splits a string into an array and removes all chars that were part of the array setup ( " \ [ ] )
   * It changes the array type to the values given
   * @param data Dataset to read
   * @param columnName column to change
   * @param dataType the type of the array
   * @return The new dataset with the changed info
   */
  public static Dataset<Row> getSplitTerms(Dataset<Row> data, String columnName, DataType dataType) {
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
  public static Dataset<Row> getSplitTerms(Dataset<Row> data, String columnNameOriginal, String columnNameNew, DataType dataType) {
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
  public static Dataset<Row> getFirstTerms(Dataset<Row> data, String columnName, DataType dataType) {
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
  public static Dataset<Row> getFirstTerms(Dataset<Row> data, String columnNameOriginal, String columnNameNew, DataType dataType) {
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
  public static Dataset<Row> getFirstNterms(Dataset<Row> data, String columnNameOriginal, String columnNameNew, DataType dataType, int numberOfItems) {
    return data.withColumn(columnNameOriginal ,substring_index(
        regexp_replace(data.col(columnNameNew), "[\\\\\"\\[\\]]+", ""), ", ", numberOfItems).cast( dataType));
  }

  public static Dataset<Row> removeArrays(Dataset<Row> data, String columnNameOriginal, String columnNameNew, DataType dataType) {
    return data.withColumn(columnNameOriginal , regexp_replace(data.col(columnNameNew), "[\\\\\"\\[\\]]+", "").cast( dataType));
  }

  public static double[] combineDoubles(double[] array1, double[] array2){

    return ArrayUtils.addAll(array1, array2);
  }

  public static Dataset<Row> limitDataSet(Dataset<Row> data, String[] columns, int[] sizes){

    if(columns.length != sizes.length){
      throw new IllegalArgumentException("The size of the columns and size to change need to be equal");
    }

    Dataset<Row> rt =  data;

    for (int i = 0; i < columns.length; i++) {
      rt = RowParser.getFirstNterms(rt, columns[i], columns[i], DataTypes.DoubleType, sizes[i]);
    }
    return rt;
  }

  public static Dataset<Row> makeDoubleArrays(Dataset<Row> data, String[] columns){

    Dataset<Row> rt =  data;

    for (int i = 0; i < columns.length; i++) {
      rt = RowParser.getSplitTerms(rt, columns[i], DataTypes.DoubleType);
    }
    return rt;
  }

  public static Dataset<Row> processDataArray(Dataset<Row> data, String[] columns, int[] sizes){
    Dataset<Row> rt =  data;

    for (int i = 0; i < columns.length; i++) {
      rt = RowParser.getFirstNterms(rt, columns[i], columns[i], DataTypes.DoubleType, sizes[i]);
      rt = RowParser.getSplitTerms(rt, columns[i], DataTypes.DoubleType);
    }
    return rt;
  }
}
