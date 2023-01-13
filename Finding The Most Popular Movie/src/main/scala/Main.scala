import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    //  Define constant
    val top = 10
    val joinColumn = "movieId"
    val sortColumn = "rating"
    val criteriaColumn = "rating"

    //  Define data file names
    val ratingFile = "./src/main/data/ml-latest-small/ratings.csv"
    val movieFile = "./src/main/data/ml-latest-small/movies.csv"
    val tagFile = "./src/main/data/ml-latest-small/tags.csv"

    //  create spark session
    val spark = SparkSession.builder()
      .appName("PopularMovies")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // load data and read the top 10 of each files
    val rating = new ReadData(spark, ratingFile)
    println("rating: " + rating.getSize)
    rating.showTop(top)

    val movie = new ReadData(spark, movieFile)
    println("movie: " + movie.getSize)
    movie.showTop(top)

    val tag = new ReadData(spark, tagFile)
    println("tag: " + tag.getSize)
    tag.showTop(top)

    // Start querying
    val query = new Query(rating.getData, movie.getData, tag.getData, joinColumn, sortColumn, criteriaColumn)
    query.getResult.show()

    spark.stop()
  }
}