import java.io._

import org.apache.avro.reflect.Union
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object App {
   Logger.getLogger("org").setLevel(Level.OFF)
   Logger.getLogger("").setLevel(Level.OFF)

   case class Movie(id: String, title: String, cast:List[String], crew: List[String], budget: Int,
                    genres: List[String], keyWords: List[String], productionCompanies: List[String], popularity: Double, voteAverage: Double)

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
//
      //val peopleDFCsv = spark.read().option("header", true)
      println("Parsing data...")

      val credits = sc.textFile("./src/data2/tmdb_5000_credits_tab.txt").map(f => {f.split("\t")})
        .map(f => (f(0).trim(), (f(1).trim(), f(2).trim(), f(3).trim())))// (movie_id, title, cast, crew)

      //budget,genres,homepage,id,keywords,original_language,original_title,overview,popularity,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,vote_average,vote_count
      val movies = sc.textFile("./src/data2/tmdb_5000_movies_tab.txt").map(f => {f.split("\t")})//.foreach(f => println(f.size))
        .map(f => (f(0).trim(), (f(1).trim(), f(2).trim(), f(3).trim(), f(4).trim(), f(5).trim(), f(6).trim())))// (movie_id, budget, genres, keywords, production_companies, popularity, vote_average)
      //val castA = parseList("""[{"cast_id": 242, "character": "Jake Sully", "credit_id": "5602a8a7c3a3685532001c9a", "gender": 2, "id": 65731, "name": "Sam Worthington", "order": 0}, {"cast_id": 3, "character": "Neytiri", "credit_id": "52fe48009251416c750ac9cb", "gender": 1, "id": 8691, "name": "Zoe Saldana", "order": 1}, {"cast_id": 25, "character": "Dr. Grace Augustine", "credit_id": "52fe48009251416c750aca39", "gender": 1, "id": 10205, "name": "Sigourney Weaver", "order": 2}, {"cast_id": 4, "character": "Col. Quaritch", "credit_id": "52fe48009251416c750ac9cf", "gender": 2, "id": 32747, "name": "Stephen Lang", "order": 3}, {"cast_id": 5, "character": "Trudy Chacon", "credit_id": "52fe48009251416c750ac9d3", "gender": 1, "id": 17647, "name": "Michelle Rodriguez", "order": 4}, {"cast_id": 8, "character": "Selfridge", "credit_id": "52fe48009251416c750ac9e1", "gender": 2, "id": 1771, "name": "Giovanni Ribisi", "order": 5}, {"cast_id": 7, "character": "Norm Spellman", "credit_id": "52fe48009251416c750ac9dd", "gender": 2, "id": 59231, "name": "Joel David Moore", "order": 6}, {"cast_id": 9, "character": "Moat", "credit_id": "52fe48009251416c750ac9e5", "gender": 1, "id": 30485, "name": "CCH Pounder", "order": 7}, {"cast_id": 11, "character": "Eytukan", "credit_id": "52fe48009251416c750ac9ed", "gender": 2, "id": 15853, "name": "Wes Studi", "order": 8}, {"cast_id": 10, "character": "Tsu'Tey", "credit_id": "52fe48009251416c750ac9e9", "gender": 2, "id": 10964, "name": "Laz Alonso", "order": 9}, {"cast_id": 12, "character": "Dr. Max Patel", "credit_id": "52fe48009251416c750ac9f1", "gender": 2, "id": 95697, "name": "Dileep Rao", "order": 10}, {"cast_id": 13, "character": "Lyle Wainfleet", "credit_id": "52fe48009251416c750ac9f5", "gender": 2, "id": 98215, "name": "Matt Gerald", "order": 11}, {"cast_id": 32, "character": "Private Fike", "credit_id": "52fe48009251416c750aca5b", "gender": 2, "id": 154153, "name": "Sean Anthony Moran", "order": 12}, {"cast_id": 33, "character": "Cryo Vault Med Tech", "credit_id": "52fe48009251416c750aca5f", "gender": 2, "id": 397312, "name": "Jason Whyte", "order": 13}, {"cast_id": 34, "character": "Venture Star Crew Chief", "credit_id": "52fe48009251416c750aca63", "gender": 2, "id": 42317, "name": "Scott Lawrence", "order": 14}, {"cast_id": 35, "character": "Lock Up Trooper", "credit_id": "52fe48009251416c750aca67", "gender": 2, "id": 986734, "name": "Kelly Kilgour", "order": 15}, {"cast_id": 36, "character": "Shuttle Pilot", "credit_id": "52fe48009251416c750aca6b", "gender": 0, "id": 1207227, "name": "James Patrick Pitt", "order": 16}, {"cast_id": 37, "character": "Shuttle Co-Pilot", "credit_id": "52fe48009251416c750aca6f", "gender": 0, "id": 1180936, "name": "Sean Patrick Murphy", "order": 17}, {"cast_id": 38, "character": "Shuttle Crew Chief", "credit_id": "52fe48009251416c750aca73", "gender": 2, "id": 1019578, "name": "Peter Dillon", "order": 18}, {"cast_id": 39, "character": "Tractor Operator / Troupe", "credit_id": "52fe48009251416c750aca77", "gender": 0, "id": 91443, "name": "Kevin Dorman", "order": 19}, {"cast_id": 40, "character": "Dragon Gunship Pilot", "credit_id": "52fe48009251416c750aca7b", "gender": 2, "id": 173391, "name": "Kelson Henderson", "order": 20}, {"cast_id": 41, "character": "Dragon Gunship Gunner", "credit_id": "52fe48009251416c750aca7f", "gender": 0, "id": 1207236, "name": "David Van Horn", "order": 21}, {"cast_id": 42, "character": "Dragon Gunship Navigator", "credit_id": "52fe48009251416c750aca83", "gender": 0, "id": 215913, "name": "Jacob Tomuri", "order": 22}, {"cast_id": 43, "character": "Suit #1", "credit_id": "52fe48009251416c750aca87", "gender": 0, "id": 143206, "name": "Michael Blain-Rozgay", "order": 23}, {"cast_id": 44, "character": "Suit #2", "credit_id": "52fe48009251416c750aca8b", "gender": 2, "id": 169676, "name": "Jon Curry", "order": 24}, {"cast_id": 46, "character": "Ambient Room Tech", "credit_id": "52fe48009251416c750aca8f", "gender": 0, "id": 1048610, "name": "Luke Hawker", "order": 25}, {"cast_id": 47, "character": "Ambient Room Tech / Troupe", "credit_id": "52fe48009251416c750aca93", "gender": 0, "id": 42288, "name": "Woody Schultz", "order": 26}, {"cast_id": 48, "character": "Horse Clan Leader", "credit_id": "52fe48009251416c750aca97", "gender": 2, "id": 68278, "name": "Peter Mensah", "order": 27}, {"cast_id": 49, "character": "Link Room Tech", "credit_id": "52fe48009251416c750aca9b", "gender": 0, "id": 1207247, "name": "Sonia Yee", "order": 28}, {"cast_id": 50, "character": "Basketball Avatar / Troupe", "credit_id": "52fe48009251416c750aca9f", "gender": 1, "id": 1207248, "name": "Jahnel Curfman", "order": 29}, {"cast_id": 51, "character": "Basketball Avatar", "credit_id": "52fe48009251416c750acaa3", "gender": 0, "id": 89714, "name": "Ilram Choi", "order": 30}, {"cast_id": 52, "character": "Na'vi Child", "credit_id": "52fe48009251416c750acaa7", "gender": 0, "id": 1207249, "name": "Kyla Warren", "order": 31}, {"cast_id": 53, "character": "Troupe", "credit_id": "52fe48009251416c750acaab", "gender": 0, "id": 1207250, "name": "Lisa Roumain", "order": 32}, {"cast_id": 54, "character": "Troupe", "credit_id": "52fe48009251416c750acaaf", "gender": 1, "id": 83105, "name": "Debra Wilson", "order": 33}, {"cast_id": 57, "character": "Troupe", "credit_id": "52fe48009251416c750acabb", "gender": 0, "id": 1207253, "name": "Chris Mala", "order": 34}, {"cast_id": 55, "character": "Troupe", "credit_id": "52fe48009251416c750acab3", "gender": 0, "id": 1207251, "name": "Taylor Kibby", "order": 35}, {"cast_id": 56, "character": "Troupe", "credit_id": "52fe48009251416c750acab7", "gender": 0, "id": 1207252, "name": "Jodie Landau", "order": 36}, {"cast_id": 58, "character": "Troupe", "credit_id": "52fe48009251416c750acabf", "gender": 0, "id": 1207254, "name": "Julie Lamm", "order": 37}, {"cast_id": 59, "character": "Troupe", "credit_id": "52fe48009251416c750acac3", "gender": 0, "id": 1207257, "name": "Cullen B. Madden", "order": 38}, {"cast_id": 60, "character": "Troupe", "credit_id": "52fe48009251416c750acac7", "gender": 0, "id": 1207259, "name": "Joseph Brady Madden", "order": 39}, {"cast_id": 61, "character": "Troupe", "credit_id": "52fe48009251416c750acacb", "gender": 0, "id": 1207262, "name": "Frankie Torres", "order": 40}, {"cast_id": 62, "character": "Troupe", "credit_id": "52fe48009251416c750acacf", "gender": 1, "id": 1158600, "name": "Austin Wilson", "order": 41}, {"cast_id": 63, "character": "Troupe", "credit_id": "52fe48019251416c750acad3", "gender": 1, "id": 983705, "name": "Sara Wilson", "order": 42}, {"cast_id": 64, "character": "Troupe", "credit_id": "52fe48019251416c750acad7", "gender": 0, "id": 1207263, "name": "Tamica Washington-Miller", "order": 43}, {"cast_id": 65, "character": "Op Center Staff", "credit_id": "52fe48019251416c750acadb", "gender": 1, "id": 1145098, "name": "Lucy Briant", "order": 44}, {"cast_id": 66, "character": "Op Center Staff", "credit_id": "52fe48019251416c750acadf", "gender": 2, "id": 33305, "name": "Nathan Meister", "order": 45}, {"cast_id": 67, "character": "Op Center Staff", "credit_id": "52fe48019251416c750acae3", "gender": 0, "id": 1207264, "name": "Gerry Blair", "order": 46}, {"cast_id": 68, "character": "Op Center Staff", "credit_id": "52fe48019251416c750acae7", "gender": 2, "id": 33311, "name": "Matthew Chamberlain", "order": 47}, {"cast_id": 69, "character": "Op Center Staff", "credit_id": "52fe48019251416c750acaeb", "gender": 0, "id": 1207265, "name": "Paul Yates", "order": 48}, {"cast_id": 70, "character": "Op Center Duty Officer", "credit_id": "52fe48019251416c750acaef", "gender": 0, "id": 1207266, "name": "Wray Wilson", "order": 49}, {"cast_id": 71, "character": "Op Center Staff", "credit_id": "52fe48019251416c750acaf3", "gender": 2, "id": 54492, "name": "James Gaylyn", "order": 50}, {"cast_id": 72, "character": "Dancer", "credit_id": "52fe48019251416c750acaf7", "gender": 0, "id": 1207267, "name": "Melvin Leno Clark III", "order": 51}, {"cast_id": 73, "character": "Dancer", "credit_id": "52fe48019251416c750acafb", "gender": 0, "id": 1207268, "name": "Carvon Futrell", "order": 52}, {"cast_id": 74, "character": "Dancer", "credit_id": "52fe48019251416c750acaff", "gender": 0, "id": 1207269, "name": "Brandon Jelkes", "order": 53}, {"cast_id": 75, "character": "Dancer", "credit_id": "52fe48019251416c750acb03", "gender": 0, "id": 1207270, "name": "Micah Moch", "order": 54}, {"cast_id": 76, "character": "Dancer", "credit_id": "52fe48019251416c750acb07", "gender": 0, "id": 1207271, "name": "Hanniyah Muhammad", "order": 55}, {"cast_id": 77, "character": "Dancer", "credit_id": "52fe48019251416c750acb0b", "gender": 0, "id": 1207272, "name": "Christopher Nolen", "order": 56}, {"cast_id": 78, "character": "Dancer", "credit_id": "52fe48019251416c750acb0f", "gender": 0, "id": 1207273, "name": "Christa Oliver", "order": 57}, {"cast_id": 79, "character": "Dancer", "credit_id": "52fe48019251416c750acb13", "gender": 0, "id": 1207274, "name": "April Marie Thomas", "order": 58}, {"cast_id": 80, "character": "Dancer", "credit_id": "52fe48019251416c750acb17", "gender": 0, "id": 1207275, "name": "Bravita A. Threatt", "order": 59}, {"cast_id": 81, "character": "Mining Chief (uncredited)", "credit_id": "52fe48019251416c750acb1b", "gender": 0, "id": 1207276, "name": "Colin Bleasdale", "order": 60}, {"cast_id": 82, "character": "Veteran Miner (uncredited)", "credit_id": "52fe48019251416c750acb1f", "gender": 0, "id": 107969, "name": "Mike Bodnar", "order": 61}, {"cast_id": 83, "character": "Richard (uncredited)", "credit_id": "52fe48019251416c750acb23", "gender": 0, "id": 1207278, "name": "Matt Clayton", "order": 62}, {"cast_id": 84, "character": "Nav'i (uncredited)", "credit_id": "52fe48019251416c750acb27", "gender": 1, "id": 147898, "name": "Nicole Dionne", "order": 63}, {"cast_id": 85, "character": "Trooper (uncredited)", "credit_id": "52fe48019251416c750acb2b", "gender": 0, "id": 1207280, "name": "Jamie Harrison", "order": 64}, {"cast_id": 86, "character": "Trooper (uncredited)", "credit_id": "52fe48019251416c750acb2f", "gender": 0, "id": 1207281, "name": "Allan Henry", "order": 65}, {"cast_id": 87, "character": "Ground Technician (uncredited)", "credit_id": "52fe48019251416c750acb33", "gender": 2, "id": 1207282, "name": "Anthony Ingruber", "order": 66}, {"cast_id": 88, "character": "Flight Crew Mechanic (uncredited)", "credit_id": "52fe48019251416c750acb37", "gender": 0, "id": 1207283, "name": "Ashley Jeffery", "order": 67}, {"cast_id": 14, "character": "Samson Pilot", "credit_id": "52fe48009251416c750ac9f9", "gender": 0, "id": 98216, "name": "Dean Knowsley", "order": 68}, {"cast_id": 89, "character": "Trooper (uncredited)", "credit_id": "52fe48019251416c750acb3b", "gender": 0, "id": 1201399, "name": "Joseph Mika-Hunt", "order": 69}, {"cast_id": 90, "character": "Banshee (uncredited)", "credit_id": "52fe48019251416c750acb3f", "gender": 0, "id": 236696, "name": "Terry Notary", "order": 70}, {"cast_id": 91, "character": "Soldier (uncredited)", "credit_id": "52fe48019251416c750acb43", "gender": 0, "id": 1207287, "name": "Kai Pantano", "order": 71}, {"cast_id": 92, "character": "Blast Technician (uncredited)", "credit_id": "52fe48019251416c750acb47", "gender": 0, "id": 1207288, "name": "Logan Pithyou", "order": 72}, {"cast_id": 93, "character": "Vindum Raah (uncredited)", "credit_id": "52fe48019251416c750acb4b", "gender": 0, "id": 1207289, "name": "Stuart Pollock", "order": 73}, {"cast_id": 94, "character": "Hero (uncredited)", "credit_id": "52fe48019251416c750acb4f", "gender": 0, "id": 584868, "name": "Raja", "order": 74}, {"cast_id": 95, "character": "Ops Centreworker (uncredited)", "credit_id": "52fe48019251416c750acb53", "gender": 0, "id": 1207290, "name": "Gareth Ruck", "order": 75}, {"cast_id": 96, "character": "Engineer (uncredited)", "credit_id": "52fe48019251416c750acb57", "gender": 0, "id": 1062463, "name": "Rhian Sheehan", "order": 76}, {"cast_id": 97, "character": "Col. Quaritch's Mech Suit (uncredited)", "credit_id": "52fe48019251416c750acb5b", "gender": 0, "id": 60656, "name": "T. J. Storm", "order": 77}, {"cast_id": 98, "character": "Female Marine (uncredited)", "credit_id": "52fe48019251416c750acb5f", "gender": 0, "id": 1207291, "name": "Jodie Taylor", "order": 78}, {"cast_id": 99, "character": "Ikran Clan Leader (uncredited)", "credit_id": "52fe48019251416c750acb63", "gender": 1, "id": 1186027, "name": "Alicia Vela-Bailey", "order": 79}, {"cast_id": 100, "character": "Geologist (uncredited)", "credit_id": "52fe48019251416c750acb67", "gender": 0, "id": 1207292, "name": "Richard Whiteside", "order": 80}, {"cast_id": 101, "character": "Na'vi (uncredited)", "credit_id": "52fe48019251416c750acb6b", "gender": 0, "id": 103259, "name": "Nikie Zambo", "order": 81}, {"cast_id": 102, "character": "Ambient Room Tech / Troupe", "credit_id": "52fe48019251416c750acb6f", "gender": 1, "id": 42286, "name": "Julene Renee", "order": 82}]""")

      var dataset = credits.join(movies)
        .map({case (id, ((title, cast, crew), (budget, genres, keywords, productC, popularity, voteAverage)))
        => Movie(id, title, parseList(cast), getCrew(crew), budget.toInt, parseList(genres), parseList(keywords),
             parseList(productC), popularity.toDouble, voteAverage.toDouble*10)}).persist()
      println("Done Parsing")

      
      /* this is computing test results on 10 different 80/20 splits. 
            ToDo: Need to incorporate MSE and SE results
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
      */

//      println("Running Trials Now")
//      for (i <- 1 to 10) {
//         println(f"Running trial $i...")
//         val test = dataset.sample(withReplacement = false, .2)
//         dataset = dataset.subtract(test) // took out collect for test
//         val result = test.collect()
//           .filter(x => x.voteAverage != 0.0) // good
//           .map(x => (x.title, x.voteAverage, KNN(x, dataset, n)))
//           .map({ case (title, real, (pred, movies)) => (title, real, movies, pred, Math.abs(real - pred)) })
//         //         result.foreach({case (movie, real, movies, pred, diff) =>
//         //            println(f"Movie: $movie%-60s, Real: $real, Pred: $pred%2.2f, SE: $diff%2.2f")
//         //            println(movies)})        // to print out all movie results from testing sample
//         val differences = result.map({ case (movie, real, movies, pred, diff) => diff })
//         val avgDiff = differences.sum / differences.length
//         val medianDiff = differences.sorted.drop(differences.length / 2).head
//         println(f"trial $i results:")
//         println(f"\t average score difference: $avgDiff")
//         println(f"\t median score difference: $medianDiff")
//      }

      val weightLogger = new BufferedWriter(new FileWriter(new File("weights.txt")))

      for (cast <- 1 to 3) {
         CAST_WEIGHT = 1 / cast
         for (crew <- 1 to 3) {
            CREW_WEIGHT = 1 / crew
            for (budget <- 1 to 3) {
               BUDGET_WEIGHT = 1 / budget
               for (genre <- 1 to 3) {
                  GENRE_WEIGHT = 1 / genre
                  for (keyword <- 1 to 3) {
                     KEYWORD_WEIGHT = 1 / keyword
                     for (prod <- 1 to 3) {
                        PRODUCTION_COMP_WEIGHT = 1 / prod
                        // do it all
                        println("--------------------------------")
                        printWeights()
                        var averagesForTrials: List[Double] = List()      // will sum median with mean in results
                        var mediansForTrials: List[Double] = List()
                        for (i <- 1 to 3) {
                           println(f"Running trial $i...")
                           val test = dataset.sample(withReplacement = false, .2) // might wanna change prop here for speed
                           dataset = dataset.subtract(test) // took out collect for test
                           val result = test.collect()
                             .filter(x => x.voteAverage != 0.0) // good
                             .map(x => (x.title, x.voteAverage, KNN(x, dataset, n)))
                             .map({ case (title, real, (pred, movies)) => (title, real, movies, pred, Math.abs(real - pred)) })
                           //         result.foreach({case (movie, real, movies, pred, diff) =>
                           //            println(f"Movie: $movie%-60s, Real: $real, Pred: $pred%2.2f, SE: $diff%2.2f")
                           //            println(movies)})        // to print out all movie results from testing sample
                           val differences = result.map({ case (movie, real, movies, pred, diff) => diff })
                           val avgDiff = differences.sum / differences.length
                           val medianDiff = differences.sorted.drop(differences.length / 2).head
                           averagesForTrials = averagesForTrials:::List(avgDiff)
                           mediansForTrials = mediansForTrials:::List(medianDiff)

                           println(f"trial $i results:")
                           println(f"\t average score difference: $avgDiff")
                           println(f"\t median score difference: $medianDiff")
                        }
                        val avgMean = averagesForTrials.sum / averagesForTrials.length
                        val avgMedian = mediansForTrials.sum / mediansForTrials.length
                        weightLogger.write(CAST_WEIGHT + "," + CREW_WEIGHT + "," + BUDGET_WEIGHT + "," +
                          GENRE_WEIGHT + "," + KEYWORD_WEIGHT + "," + PRODUCTION_COMP_WEIGHT + "," +
                          avgMean + "," + avgMedian)
                        println(CAST_WEIGHT + "," + CREW_WEIGHT + "," + BUDGET_WEIGHT + "," +
                          GENRE_WEIGHT + "," + KEYWORD_WEIGHT + "," + PRODUCTION_COMP_WEIGHT + "," +
                          avgMean + "," + avgMedian)
                     }
                  }
               }
            }
         }
      }
      weightLogger.close()

//      println("Running Trials Now")
//      for (i <- 1 to 10) {
//         println(f"Running trial $i...")
//         val test = dataset.sample(withReplacement = false, .2)
//         dataset = dataset.subtract(test) // took out collect for test
//         val result = test.collect()
//           .filter(x => x.voteAverage != 0.0) // good
//           .map(x => (x.title, x.voteAverage, KNN(x, dataset, n)))
//           .map({ case (title, real, (pred, movies)) => (title, real, movies, pred, Math.abs(real - pred)) })
//         //         result.foreach({case (movie, real, movies, pred, diff) =>
//         //            println(f"Movie: $movie%-60s, Real: $real, Pred: $pred%2.2f, SE: $diff%2.2f")
//         //            println(movies)})        // to print out all movie results from testing sample
//         val differences = result.map({ case (movie, real, movies, pred, diff) => diff })
//         val avgDiff = differences.sum / differences.length
//         val medianDiff = differences.sorted.drop(differences.length / 2).head
//         println(f"trial $i results:")
//         println(f"\t average score difference: $avgDiff")
//         println(f"\t median score difference: $medianDiff")
//      }

//      println("Running Trials Now")
//      for (i <- 1 to 4) {
//         println(f"Running trial $i...")
//         val test = dataset.filter(t => t.title.trim() == "Avatar").collect().toList
//         dataset = dataset.filter(x => !test.contains(x))
//         val result = test
//           .filter(x => x.voteAverage != 0.0)
//           .map(x => (x.title, x.voteAverage, KNN(x, dataset, n)))
//           .map({case (title, real, (pred, movies)) => (title, real, movies, pred, Math.pow((real - pred), 2))})
//         result.foreach({case (movie, real, movies, pred, diff) =>
//            println(f"Movie: $movie%-60s, Real: $real, Pred: $pred%2.2f, SE: $diff%2.2f")
//            println(movies)})
//         val diffs = result.map({case (movie, real, movies, pred, diff) => diff}).toList
//         println("MSE: ", (diffs.sum / diffs.size))
//      }
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

      return PRODUCTION_COMP_WEIGHT * diffScore
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
      return  ( 100 - (((totalSimilar - numSimilar) / Math.max(minSize + maxSize / 2.0, 1)) * 100) ) * KEYWORD_WEIGHT
   }
   
   
      /*
     Computes the cosine distance similarity between two non-zero vectors. Formula can
     be used to approximate the similarity of two given list of strings.
     difference is 1 - similarity.
    */
   def getKeywordDifferenceCosine(movieA: Movie, movieB : Movie) : Double = {
      val movieAkeys = movieB.keyWords.map(keyA => (keyA, 1)).groupBy(x => x._1).mapValues(x => x.map(x => x._2)).mapValues(x => x.sum)
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

   def printWeights(): Unit = {
      println("CAST_WEIGHT = " + CAST_WEIGHT)
      println("CREW_WEIGHT = " + CREW_WEIGHT)
      println("BUDGET_WEIGHT = " + BUDGET_WEIGHT)
      println("GENRE_WEIGHT = " + GENRE_WEIGHT)
      println("KEYWORD_WEIGHT = " + KEYWORD_WEIGHT)
      println("PRODUCTION_COMP_WEIGHT = " + PRODUCTION_COMP_WEIGHT)
   }




   //   def getCastDifferenceScore(a, b:)
//   def getProductionDifferenceScore(a, b:)
//   def getGenreDifferenceScore

}
