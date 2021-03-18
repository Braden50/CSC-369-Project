import org.apache.avro.reflect.Union
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object App {
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("").setLevel(Level.OFF)

   case class Movie(var id: String, title: String, cast:List[String], crew: List[String], budget: Int,
                    genres: List[String], keyWords: List[String], productionCompanies: List[String], popularity: Double, voteAverage: Double)

   def emptyMovie():Movie = Movie("","",List.empty[String], List.empty[String], 0, List.empty[String],List.empty[String],
      List.empty[String], 0.0, 0.0)

   var CAST_WEIGHT = 1.0
   var CREW_WEIGHT = 1.0
   var BUDGET_WEIGHT = 1.0
   var GENRE_WEIGHT = 1.0
   var KEYWORD_WEIGHT = 1.0
   var PRODUCTION_COMP_WEIGHT = 1.0

   def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("MovieRatingsPredictor").setMaster("local[4]")
      val sc = new SparkContext(conf);
      val n = 10
      println("Parsing data...")

      val credits = sc.textFile("./src/data2/tmdb_5000_credits_tab.txt").map(f => {
         f.split("\t")
      })
        .map(f => (f(0).trim(), (f(1).trim(), f(2).trim(), f(3).trim())))
      // (movie_id, title, cast, crew)

      val movies = sc.textFile("./src/data2/tmdb_5000_movies_tab.txt").map(f => {
         f.split("\t")
      })
        .map(f => (f(0).trim(), (f(1).trim(), f(2).trim(), f(3).trim(), f(4).trim(), f(5).trim(), f(6).trim())))
      // (movie_id, budget, genres, keywords, production_companies, popularity, vote_average)

      var dataset = credits.join(movies)
        .map({ case (id, ((title, cast, crew), (budget, genres, keywords, productC, popularity, voteAverage)))
        => Movie(id, title, parseList(cast), getCrew(crew), budget.toInt, parseList(genres), parseList(keywords),
           parseList(productC), popularity.toDouble, voteAverage.toDouble * 10)
        }).persist()
      println("Done Parsing")
      test_input(dataset)

   }
   def test_input(dataset: RDD[Movie]){
      var n = 10
      println("Movie Name:")
      val title = readLine().trim()
      println("Cast Members:")
      val cast = readLine().trim().split(",").toList
      println("Crew Members:")
      val crew = readLine().trim.split(",").toList
      println("Budget:")
      val budget = readLine().trim.toInt
      println("Genres:")
      val genres = readLine().trim.split(",").toList
      println("Keywords:")
      val keywords = readLine().trim.split(",").toList
      println("Production Companies:")
      val prodCompanies = readLine().trim.split(",").toList

      val inputMovie = Movie("input", title, cast, crew, budget, genres, keywords, prodCompanies, 0.0, 0.0)
      val result = KNN(inputMovie, dataset,n)
      println(f"Expected Rating: ${result._1}")
      println("Similar Movies:")
      result._2.foreach(println)

   }

   def test_2(dataset: RDD[Movie]){
      var n = 10
      for(i <- 1 to 10){
         var train = dataset.sample(withReplacement = false, .2)
         var trainList =  dataset.sample(withReplacement = false, .2).collect()
         var test = dataset.filter(movie => !trainList.contains(movie)).collect()
         val result = test.filter(x => x.voteAverage != 0).map(inputmovie => (inputmovie.title, inputmovie.voteAverage, KNN(inputmovie, train, n)))
           .map({case (movieTitle, real, (pred, movies)) => (movieTitle, math.abs(real-pred), movies)})
         println("Model " + i + " results")
         val sum = result.map({case (movieTitle, diff, movies) => (diff)}).sum
         val count = result.map({case (movieTitle, diff, movies) => 1}).sum
         println("Difference from expected on average is : " + (sum/count))
      }
   }

   def test_3(data: RDD[Movie], n: Int) {
      var n = 10
      var dataset = data
      println("Running Trials Now")
      for (i <- 1 to 10) {
         println(f"Running trial $i...")
         val test = dataset.sample(withReplacement = false, .2)
         dataset = dataset.subtract(test)
         val result = test.collect()
           .filter(x => x.voteAverage != 0.0)
           .map(x => (x.title, x.voteAverage, KNN(x, dataset, n)))
           .map({ case (title, real, (pred, movies)) => (title, real, movies, pred, Math.abs(real - pred)) })
         val differences = result.map({ case (movie, real, movies, pred, diff) => diff })
         val avgDiff = differences.sum / differences.length
         val medianDiff = differences.sorted.drop(differences.length / 2).head
         println(f"trial $i results:")
         println(f"\t average score difference: $avgDiff")
         println(f"\t median score difference: $medianDiff")
      }
   }

   def test_4(data:RDD[Movie]) {
      var n = 10
      var dataset = data
      println("Running Trials Now")
      for (i <- 1 to 4) {
         println(f"Running trial $i...")
         val test = dataset.filter(t => t.title.trim() == "Avatar").collect().toList
         dataset = dataset.filter(x => !test.contains(x))
         val result = test
           .filter(x => x.voteAverage != 0.0)
           .map(x => (x.title, x.voteAverage, KNN(x, dataset, n)))
           .map({ case (title, real, (pred, movies)) => (title, real, movies, pred, Math.pow((real - pred), 2)) })
         result.foreach({ case (movie, real, movies, pred, diff) =>
            println(f"Movie: $movie%-60s, Real: $real, Pred: $pred%2.2f, SE: $diff%2.2f")
            println(movies)
         })
         val diffs = result.map({ case (movie, real, movies, pred, diff) => diff }).toList
         println("MSE: ", (diffs.sum / diffs.size))
      }
   }

   def KNN(inputMovie: Movie, data: RDD[Movie], n: Int): (Double, List[String]) = {
      val movies = data.map(movie => (movie, euclidean_distance(movie, inputMovie)))
        .sortBy(movie => movie._2).take(n)
      val rating = movies.map({case (movie, dist) => movie.voteAverage})
        .aggregate((0.0, 0.0))((x, y) => (x._1 + y, x._2+ 1), (x, y) => (x._1 + y._1, x._2 + y._2))
      if(rating._2 == 0){
         return (0.0, List(""))
      }
      return (rating._1/rating._2, movies.map({case (m, c) => m.title}).toList)
   }


   def getCrew(crew: String): List[String] = {
      val parsed = JSON.parseFull(crew).get.asInstanceOf[List[Map[String, String]]]
      val important = List("Producer", "Director", "Writer")
      val ret = parsed.toStream.filter(x => important.contains(x("job")))
        .map(x => x("name")).toList
      return ret.take(20)
   }

   def parseList(lst: String): List[String] = {
      val parsed = JSON.parseFull(lst).get.asInstanceOf[List[Map[String, String]]]
      val ret = parsed.toStream.map(x => x("name")).toList
      return ret.take(20)
   }



   def getCastDifference(movieA: Movie, movieB: Movie): Double = {
      var x = 1.0

      movieA.cast.foreach(a => {
         val i = movieB.cast.indexOf(a)
         if(i == -1){
            x += 0
         } else {
            x += 100.0/Math.max(Math.min(movieA.cast.size, movieB.cast.size), 1)
         }
      })
      return ( 100.0/x ) * CAST_WEIGHT
   }

   def getCrewDifference(movieA: Movie, movieB: Movie): Double = {
      var x = 1.0

      movieA.crew.foreach(a => {
         val i = movieB.crew.indexOf(a)
         if(i == -1){
            x += 0
         } else {
            x += 100.0/Math.max(Math.min(movieA.crew.size, movieB.crew.size), 1)
         }
      })
      return ( 100.0/x ) * CREW_WEIGHT
   }

   def getBudgetDifference(movieA : Movie, movieB: Movie): Double = {
      // help from: https://stats.stackexchange.com/questions/281162/scale-a-number-between-a-range
      // NOTE: 1/5 of the data has a budget of 0, inaccurate, not actually 0
      val r_min = 0
      val r_max = 80000000  // max budget in dataset, hardcoded
      val t_min = 0
      val t_max = 1
      val budget_diff = Math.abs(movieA.budget - movieB.budget)
      ( ( (budget_diff - r_min) / (r_max - r_min) ) * ( t_max - t_min ) + t_min ) * BUDGET_WEIGHT
   }

   def euclidean_distance(movieA: Movie, movieB: Movie): Double ={
      var sum_squared_distance = 0.0
      val funcs = List(
         getCastDifference _,
         getCrewDifference _,
         getGenreDifferenceScore _,
         getBudgetDifference _,
         getProductionCompaniesDifference _,
         getKeywordDifference _
      )
      for(fun <- funcs){
         sum_squared_distance += math.pow(fun(movieA, movieB), 2)
      }
      return math.sqrt(sum_squared_distance)
   }

   def getGenreDifferenceScore(movieA: Movie, movieB : Movie): Double = {
      var x = 1.0
      for(a <- movieA.genres){
         for(b <- movieB.genres){
            if(a.contains(b) || b.contains(a)){
               x += 100.0/Math.max(Math.min(movieA.genres.size, movieB.genres.size), 1)
            }
         }
      }
      return ( 100.0/x ) * GENRE_WEIGHT
   }

   def getProductionCompaniesDifference(movieA: Movie, movieB: Movie): Double = {
      // Minimum list size = maximum similar elements - vary from 0 to ~5
      // Relative similarity = Number of similar elements / Minimum list size - [0, 1]
      // Relative similarity (including extras) = Number of similar elements / Max list size - [0, 1]
      //    This is for situations like movies with prod comps: (A, B, C), (A, B, C), (A)
      //       By having this similarity as a factor, (A, B, C) is more similar to (A, B, C) than (A)
      // Results:
      //    0 - 30: very similar, either all matching or some with low options
      //    50 - 70: some matching with at least one having many options
      //    ~100: No matches
      val minSize = Math.min(movieA.productionCompanies.size, movieB.productionCompanies.size)
      val maxSize = Math.max(movieA.productionCompanies.size, movieB.productionCompanies.size)
      var numSimilar = 0
      for (a <- movieA.productionCompanies) {
         if (movieB.productionCompanies.contains(a)) {
            numSimilar += 1
         }
      }
      val similar = (numSimilar * 1.0) / Math.max(minSize, 1)
      val similarWithExtras = (numSimilar * 1.0) / Math.max(maxSize, 1)
      val diffScore = 100 - ( ( (similar * 0.7) + (similarWithExtras * 0.3) ) * 100.0 )
      // 100 - (100 * (0.7 * proportion of small list similar + 0.3 * proportion of large list similar))
      // constants represent the proportion of similarity we want to include

      return diffScore * PRODUCTION_COMP_WEIGHT
   }

   def getKeywordDifference(movieA: Movie, movieB: Movie): Double ={

      val minSize = Math.min(movieA.keyWords.size, movieB.keyWords.size)
      val maxSize = Math.max(movieA.keyWords.size, movieB.keyWords.size)
      var numSimilar = 0
      val totalSimilar = Math.min(movieA.keyWords.size, movieB.keyWords.size)
      for (a <- movieA.keyWords) {
         if (movieB.keyWords.contains(a)) {
            numSimilar += 1
         }
      }
      return ( 100 - (((numSimilar) / Math.max((minSize + maxSize) / 2.0, 1)) * 100) ) * KEYWORD_WEIGHT
   }
   
   
      /*
     Computes the cosine distance similarity between two non-zero vectors. Formula can
     be used to approximate the similarity of two given list of strings.
     difference is 1 - similarity.
    */
   def getKeywordDifferenceCosine(movieA: Movie, movieB : Movie) : Double = {
      val movieAkeys = movieA.keyWords.map(keyA => (keyA, 1)).groupBy(x => x._1).mapValues(x => x.map(x => x._2)).mapValues(x => x.sum)
      val movieBkeys = movieB.keyWords.map(keyB => (keyB, 1)).groupBy(x => x._1).mapValues(x => x.map(x => x._2)).mapValues(x => x.sum)
      val intersection = movieA.keyWords.filter(keyA => movieB.keyWords.contains(keyA))
      var numerator = intersection.map(str => (movieAkeys(str) * movieBkeys(str))).sum
      val s1 = movieAkeys.map({case (key, count) => Math.pow(count, 2)}).sum
      val s2 = movieBkeys.map({case (key, count) => Math.pow(count, 2)}).sum
      val denominator = math.sqrt(s1) * math.sqrt(s2)
      if(denominator == 0){
         return 0.0
      }
      100 - ((numerator).toDouble/denominator * 100)
   }

}
