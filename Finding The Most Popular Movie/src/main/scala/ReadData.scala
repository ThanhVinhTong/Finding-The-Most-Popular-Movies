import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadData(spark: SparkSession, filename: String) {

  private val df: DataFrame = spark.read.option("header", value = true).csv(filename)

  //  get data method
  def getData: DataFrame = {
    this.df
  }

  //  Grab the top x
  def showTop(x: Int): Unit = {
    df.show(x)
  }

  def getSize: Long = {
    this.df.count()
  }
}
