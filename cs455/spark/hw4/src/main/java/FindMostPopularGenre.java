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


public class FindMostPopularGenre implements Serializable {

  private final String artistTerms = "artist_terms";
  private final DataType stringType = DataTypes.StringType;

  private final Dataset<Row> dataFull;


  public FindMostPopularGenre(Dataset<Row> dataFull) {
    this.dataFull = dataFull;
  }

  public void run(){
    Dataset<Row> allTerms = dataFull.select(col(artistTerms), col("year")).filter((col("year")).gt(0));

    allTerms = RowParser.getSplitTerms(allTerms, artistTerms, stringType);
    allTerms  = allTerms.withColumn(
        artistTerms, explode(col(artistTerms)));

    Dataset test = allTerms.groupBy(col(artistTerms), col("year")).count();

    test.coalesce(1).orderBy(col("year").desc(), col("year").desc(), col("count").desc() ).show(); //.write().mode(SaveMode.Overwrite).format("json").save("/home/HW4_output/test/counts");;
    JavaRDD<String> terms2 = allTerms.map(row -> row.mkString(), Encoders.STRING()).javaRDD();

    //JavaPairRDD<String, Integer> ones = terms2.mapToPair(s -> new Tuple2<>(s, 1));

    //JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

    //System.out.println(Arrays.toString(counts.coalesce(1).collect().toArray()));



  }
}
