import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Query (df1: DataFrame, df2: DataFrame, df3: DataFrame, joinColumn: String, sortColumn: String) {

  private val ta = df1.alias("ta")
  private val tb = df2.alias("tb")
  private val tc = df3.alias("tc")

  private val inner_join = tb.join(tc, tb(joinColumn) === tc(joinColumn), "inner")
    .join(ta, tb(joinColumn) === ta(joinColumn), "inner")

  def getResult: DataFrame = {
    this.inner_join.orderBy(desc(sortColumn))
  }
}
