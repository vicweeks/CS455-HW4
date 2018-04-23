import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.size;

import Util.RowParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

public class FindSectionsInfo {

  private final String artistTerms = "artist_terms";
  private final DataType stringType = DataTypes.StringType;

  private final Dataset<Row> dataFull;

  public FindSectionsInfo(Dataset<Row> dataFull) {
    this.dataFull = dataFull;
  }


  public void run(){

    Dataset<Row> allTerms = dataFull.select(col("tatums_start"), col("segments_start"));

    allTerms = RowParser.getSplitTerms(allTerms, "tatums_start", DataTypes.DoubleType);
    allTerms = RowParser.getSplitTerms(allTerms, "segments_start", DataTypes.DoubleType);

    allTerms.agg(min(size(col("tatums_start"))), max(size(col("tatums_start"))),
        min(size(col("segments_start"))), max(size(col("segments_start")))).show();

  }
}
