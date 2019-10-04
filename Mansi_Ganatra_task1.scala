import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.graphframes._
import scala.collection._

object Mansi_Ganatra_task1 {


  def filterUsersByThreshold(userBusinessMap: Map[String, Set[String]], filterThreshold: Int): Map[String, Set[String]] ={
    var filteredUsers = Map[String, Set[String]]()
    userBusinessMap.map(user1 => {
      var businesses = user1._2
      var commonUsers= userBusinessMap.filter(user2 =>(user1._1 != user2._1 && user2._2.intersect(businesses).size >= filterThreshold)).keySet
      filteredUsers += user1._1 -> commonUsers
    })
    filteredUsers = filteredUsers.filter(_._2.nonEmpty)

    return filteredUsers
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 3) {
      println("Usage: ./bin/spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 –class " +
        "firstname_lastname_task1 firstname_lastname_hw4.jar <filter_tthreshold> <input_file_path> <community_output_file_path>")
      return
    }
    val filterThreshold = args(0).toInt
    val inputFilePath = args(1)
    val outputFilePath =args(2)

    Logger.getLogger("org").setLevel(Level.INFO)

    val ss = SparkSession
      .builder()
      .appName("task1")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val sc = ss.sparkContext
    import ss.implicits._

//    val inputFilePath = "ub_sample_data.csv"
//    val outputFilePath = "task1_result_scala.csv"
//    val filterThreshold = 7
    val maxIter = 5

    val start = System.currentTimeMillis()

    var userBusinessRdd = sc.textFile(inputFilePath)
        .filter(!_.contains("user_id"))
      .map(entry => entry.split(","))
      .map(entry => (entry(0), entry(1)))
      .persist()

    var userBusinessMap = userBusinessRdd.groupByKey().mapValues(entry=>entry.toSet).collectAsMap()

    var filteredUsers = Map[String, Set[String]]()
    filteredUsers =  filterUsersByThreshold(userBusinessMap, filterThreshold)

    var userVertices = filteredUsers.keySet.toList.toDF("id")

    var userTuples  = List[(String, String, String)]()
    filteredUsers.map(userEntry => {
      var user1 = userEntry._1
      var tuples = userEntry._2.map(user2=>(user1, user2, "similar")).toList
      userTuples = userTuples ++ tuples
    })
    var userEdges = sc.parallelize(userTuples).toDF("src", "dst", "relationship")

    val graph = GraphFrame(userVertices, userEdges)

    val userCommunitiesDF = graph.labelPropagation.maxIter(maxIter).run()

    val userCommunitiesRDD= userCommunitiesDF.rdd
      .map(entry=>(entry(1), entry(0)))
      .groupByKey()
      .mapValues(entry=>(entry.toList.asInstanceOf[List[String]].sorted, entry.size))
      .map(entry => entry._2)
      .sortBy(entry => (entry._2, entry._1.head))
      .map(entry => entry._1)

    val userCommunities =  userCommunitiesRDD.collect()

    val writer = new PrintWriter(new File(outputFilePath))

    for(community <- userCommunities) {
      var stringToWrite = ""
      for(user <- community.dropRight(1)) {
        stringToWrite = stringToWrite.concat("\'" + user + "\', ")
      }
      stringToWrite = stringToWrite.concat("\'" + community.last + "\'" +"\n")
      writer.write(stringToWrite)
    }
    writer.close()

    val end = System.currentTimeMillis()

    println("Duartion: " + (end-start)/1000)

  }
}
