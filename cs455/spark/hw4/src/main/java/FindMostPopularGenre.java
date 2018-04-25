import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import Util.RowParser;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;


/**
 * Finds the total count of the first terms for songs
 * This data was used to group the many terms into simplified terms
 */
public class FindMostPopularGenre implements Serializable {

  private final String artistTerms = "artist_terms";
  private final DataType stringType = DataTypes.StringType;

  private final Dataset<Row> dataFull;


  /**
   * Default constructor that takes the full data of songs
   * @param dataFull all the song data
   */
  public FindMostPopularGenre(Dataset<Row> dataFull) {
    this.dataFull = dataFull;
  }

  public void run(){
    // Get only the artist terms
    Dataset<Row> allTerms = dataFull.select(col(artistTerms));

    // Get the first term if the artist terms
    Dataset dataFixed7 = RowParser.getFirstTerms(allTerms, artistTerms, stringType);

    // Count all the terms
    Dataset test = dataFixed7.groupBy(col(artistTerms)).count();

    // Write all the counts from the other nodes
    test.coalesce(1).orderBy(col("count").desc()).write()
        .mode(SaveMode.Overwrite).format("json").save("/home/HW4_output/test/Totalcounts");


  }
}
