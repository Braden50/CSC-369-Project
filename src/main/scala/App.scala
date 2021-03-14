import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object App {
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("").setLevel(Level.OFF)


   def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("MovieRatingsPredictor").setMaster("local[4]")
      val sc = new SparkContext(conf);
//
      //val peopleDFCsv = spark.read().option("header", true)

      val credits = sc.textFile("./src/data2/tmdb_5000_credits_tab.txt").map(f => {f.split("\t")})
        .map(f => (f(0), (f(1), f(2), f(3))))// (movie_id, title, cast, crew)

      //budget,genres,homepage,id,keywords,original_language,original_title,overview,popularity,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,vote_average,vote_count
      val movies = sc.textFile("./src/data2/tmdb_5000_movies_tab.txt").map(f => {f.split("\t")})//.foreach(f => println(f.size))
        .map(f => (f(0), (f(1), f(2), f(3), f(4), f(5), f(6))))// (movie_id, budget, genres, keywords, production_companies, popularity, vote_average)

      val dataset = credits.join(movies).persist()
      //[Tom Cruise ...]

      val test = dataset.sample(false, 0.2)

      test.map(x => (x._2._1._1, calculateRating(x, dataset)))

   }

   def calculateRating(movie:(String, ((String, String, String), (String, String, String, String, String, String))),
                       data: RDD[(String, ((String, String, String), (String, String, String, String, String, String)))]): Double = {


   }



//   def getCastDifferenceScore(a, b:)
//   def getProductionDifferenceScore(a, b:)
//   def getGenreDifferenceScore

}
