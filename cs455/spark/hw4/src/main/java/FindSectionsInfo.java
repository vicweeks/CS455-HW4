import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.size;

import Util.RowParser;
import java.io.Serializable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class FindSectionsInfo implements Serializable {

  private final String artistTerms = "artist_terms";
  private final DataType stringType = DataTypes.StringType;

  private final Dataset<Row> dataFull;

  public FindSectionsInfo(Dataset<Row> dataFull) {
    this.dataFull = dataFull;
  }


  public void run(){

    Dataset<Row> allTerms = dataFull.select("bars_start", "beats_start", "segments_loudness_max",
        "segments_pitches", "tatums_start", "segments_timbre", "segments_start");

    allTerms = RowParser.makeDoubleArrays(allTerms, RowParser.doubleArraysInData);

    allTerms.agg(min(size(col("bars_start"))), min(size(col("beats_start"))), min(size(col("segments_loudness_max"))),
        min(size(col("segments_pitches"))),min(size(col("tatums_start"))),min(size(col("segments_start")))
        ,min(size(col("segments_start")))).show();


  }
}
