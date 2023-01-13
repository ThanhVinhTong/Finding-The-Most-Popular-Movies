import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Query (df1: DataFrame, df2: DataFrame, df3: DataFrame, joinColumn: String, sortColumn: String, criteria: String) {

  private var ta = df1.alias("ta")
  ta = ta.withColumn(criteria, col(criteria).cast("int")).groupBy(joinColumn).avg(criteria)
  ta.show()
  private val tb = df2.alias("tb")
  private val tc = df3.alias("tc")

  private var inner_join = tb.join(tc, tb(joinColumn) === tc(joinColumn), "inner")
    .join(ta, tb(joinColumn) === ta(joinColumn), "inner")

  def getResult: DataFrame = {
    this.inner_join.orderBy(desc("avg(%s)".format(criteria)))
  }
}
