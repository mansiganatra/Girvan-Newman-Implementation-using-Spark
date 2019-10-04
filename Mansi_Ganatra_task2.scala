import java.io.{File, PrintWriter}

import Mansi_Ganatra_task1.filterUsersByThreshold
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.graphframes._

import scala.collection.{mutable, _}

object Mansi_Ganatra_task2 {

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

  def generateBetweennessForRoot(root:String, nearByUsersMap:Map[String, Set[String]]) : Map[(String, String), Double] = {

    var edgebetweennessMap = Map[(String, String), Double]()
    var levelNodesMap = Map[String, Int]()
    var childParentsMap = Map[String, Set[String]]()
    var currentLevel =0

    var currentChildNodes = Set[String](root)

    levelNodesMap += root -> currentLevel

    while(currentChildNodes.nonEmpty){

      currentLevel += 1
      var childrenForNextIteration = Set[String]()

      for(child<-currentChildNodes){
        val currentGrandChildren = nearByUsersMap(child)

        for(grandChild <- currentGrandChildren){
          if(!levelNodesMap.contains(grandChild)){

            levelNodesMap += grandChild -> currentLevel
            childParentsMap += grandChild -> Set[String](child)
            childrenForNextIteration += grandChild
          }
          else if(levelNodesMap(grandChild) == levelNodesMap(child)+1){
            var updatedParents = childParentsMap(grandChild)+ child
            childParentsMap += grandChild -> updatedParents
          }
        }
      }

      currentChildNodes = childrenForNextIteration - root
    }

    var allNodes = levelNodesMap.keySet

    var nodepartialCreditsMap = Map[String, Double]()

    for(node <- allNodes){
      nodepartialCreditsMap += node -> 1.0
    }

    while(currentLevel > 0){
      var currentLevelNodes =  levelNodesMap.filter(_._2 == currentLevel).toSet

      for(node <- currentLevelNodes){

        var parentsOfCurrentNode = childParentsMap(node._1)
        val partialCredit =  nodepartialCreditsMap(node._1)/parentsOfCurrentNode.size

        for(parent<- parentsOfCurrentNode){
          val updatedCredit = nodepartialCreditsMap(parent) + partialCredit
          nodepartialCreditsMap += parent -> updatedCredit
          edgebetweennessMap += (node._1, parent)-> partialCredit
        }
      }

      currentLevel -= 1
    }

    return edgebetweennessMap

  }
  def generateBetweennessResult(nearByUsersMap:Map[String, Set[String]]): Map[(String, String), Double] = {

    var betweennessMap = Map[(String, String), Double]()
    var finalResult = Map[(String, String), Double]()

    var allNodes =  nearByUsersMap.keySet

    for(root <- allNodes){

      var edgesBetweennessMap =  generateBetweennessForRoot(root, nearByUsersMap)
      for(betweenness <- edgesBetweennessMap){
        var edge = betweenness._1
        var betValue = betweenness._2
        if(betweennessMap.contains(edge)){
          var newBetValue = betValue + betweennessMap(edge)
          betweennessMap += edge -> newBetValue
        }
        else {
          betweennessMap += edge -> betValue
        }
      }
    }

    for(betweenness <- betweennessMap){
      var sortedTuple =  List(betweenness._1._1, betweenness._1._2).sorted
      finalResult += (sortedTuple(0), sortedTuple(1)) -> betweenness._2/2
    }

    finalResult = mutable.ListMap[(String, String), Double](finalResult.toSeq.sortBy(entry => (entry._2,entry._1._1)):_*)


    return finalResult

  }

  def generateAdjacencyMatrix(filteredusers: Map[String, Set[String]]): Map[(String, String), Double] = {

    var users = filteredusers.keySet
    var adjacencyMatrix = Map[(String, String), Double]()

    for(u1<-users){
      for(u2<-users){
        var pair = List[String](u1,u2).sorted match {case List(a,b) => (a,b)}
        if(filteredusers(u1).contains(u2)){
          adjacencyMatrix += pair -> 1.0
        }
        else {
          adjacencyMatrix += pair -> 0.0
        }
      }
    }

    return adjacencyMatrix
  }

  def generateDegreeMatrix(filteredUsers: Map[String, Set[String]]): Map[String, Long] = {

    var users  = filteredUsers.keySet
    var degreeMatrix = Map[String, Long]()

    for(user <- users){
      degreeMatrix += user -> filteredUsers(user).size
    }

    return degreeMatrix
  }

  def generateUserClusters(filteredUsers: Map[String, Set[String]]):Set[List[String]] ={

    var adjacentUsersMapCopy = Map[String, Set[String]]() ++ filteredUsers

    var uniqueUsers  = filteredUsers.keySet

    var clusters = Set[List[String]]()

    for(user <- uniqueUsers){
      var currentMembers =  Set[String]()
      var currentCluster = Set[String]()

      if(adjacentUsersMapCopy.contains(user)){
        currentMembers = adjacentUsersMapCopy(user)
        currentCluster += user
      }

      while(currentMembers.nonEmpty){

        var membersForNextIteration =  Set[String]()

        for(currentMember <- currentMembers){
          currentCluster += currentMember
          if(adjacentUsersMapCopy.contains(currentMember)){
            membersForNextIteration = membersForNextIteration ++ adjacentUsersMapCopy(currentMember)
            adjacentUsersMapCopy -= currentMember
          }
        }

        currentMembers = membersForNextIteration - user
      }

      if(currentCluster.nonEmpty){
        clusters += currentCluster.toList.sorted
      }
    }

    return clusters
  }

  def runGirvanNewman(graph: Set[List[String]], adjacencyMatrix: Map[(String, String), Double],
                      inputDegreeMatrix: Map[String, Long], m: Int,
                      inputUserBetweennessMap: Map[(String, String), Double],
                      inputFilteredUsers: Map[String, Set[String]]): List[List[String]] ={

    var maxQ = Double.MinValue
    var maxClusters = Set[List[String]]()
    var currentQ = 0.0

    var clusters =  Set[List[String]]() ++ graph
    var degreeMatrix = Map[String, Long]() ++ inputDegreeMatrix
    var userBetweennessMap = Map[(String, String), Double]() ++ inputUserBetweennessMap
    var filteredUsers = Map[String, Set[String]]() ++ inputFilteredUsers

    while(userBetweennessMap.nonEmpty){

      var totalMinusExpected = 0.0

      for(cluster<-clusters){
        for(u1<-cluster){
          for(u2<-cluster){
            if(u1<u2){
              var aij = adjacencyMatrix((u1,u2))
              var ki= degreeMatrix(u1)
              var kj =  degreeMatrix(u2)
              var modSum = (aij - ((ki*kj)/(2*m)))
              totalMinusExpected += modSum
            }
          }
        }
      }

      currentQ = totalMinusExpected/(2*m)
      if(currentQ > maxQ){
        maxQ = currentQ
        maxClusters = clusters
      }

      val maxBetweenness = userBetweennessMap.values.max
      var edgesToDrop =  Set[(String, String)]()
      for(edge<-userBetweennessMap.keySet){

        if(userBetweennessMap(edge) ==  maxBetweenness){
          edgesToDrop += edge

          if(degreeMatrix(edge._1) > 0)
            degreeMatrix += edge._1 -> (degreeMatrix(edge._1) - 1)
          if(degreeMatrix(edge._2) >0)
            degreeMatrix += edge._2 -> (degreeMatrix(edge._2) - 1)

          filteredUsers += edge._1 -> (filteredUsers(edge._1) - edge._2)
          filteredUsers += edge._2 -> (filteredUsers(edge._2) -  edge._1)

        }
      }

      for(edge <- edgesToDrop){
        userBetweennessMap -= edge
      }

      clusters  = generateUserClusters(filteredUsers)
    }

    var sortedMaxClusters =  maxClusters.toList.sortBy(entry=>(entry.size, entry.head))

    return sortedMaxClusters

  }


  def main(args: Array[String]): Unit = {

        if(args.length != 4) {
          println("Usage: ./bin/spark-submit Mansi_Ganatra_task1.class Mansi_Ganatra_hw1.jar <input_file_path> <outputFilePath>")
          return
        }

        val filterThreshold = args(0).toInt
        val inputFilePath =  args(1)
        val outputFilePathBetweenness = args(2)
        val outputFilePathCommunities = args(3)


    Logger.getLogger("org").setLevel(Level.INFO)

    val ss = SparkSession
      .builder()
      .appName("task2")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val sc = ss.sparkContext
    import ss.implicits._

//    val inputFilePath = "ub_sample_data.csv"
//    val outputFilePathBetweenness = "task2_result_betweenness_scala.csv"
//    val outputFilePathCommunities = "task2_result_communities_scala.csv"
//    val filterThreshold = 7


    val start = System.currentTimeMillis()

    var userBusinessRdd = sc.textFile(inputFilePath)
      .filter(!_.contains("user_id"))
      .map(entry => entry.split(","))
      .map(entry => (entry(0), entry(1)))
      .persist()

    var userBusinessMap = userBusinessRdd.groupByKey().mapValues(entry=>entry.toSet).collectAsMap()

    var filteredUsers = Map[String, Set[String]]()
    filteredUsers =  filterUsersByThreshold(userBusinessMap, filterThreshold)

//    var userTuples  = List[(String, String, String)]()
//    filteredUsers.map(userEntry => {
//      var user1 = userEntry._1
//      var tuples = userEntry._2.map(user2=>(user1, user2, "similar")).toList
//      userTuples = userTuples ++ tuples
//    })

    var userBetweennessMap = Map[(String, String), Double]()

    userBetweennessMap = generateBetweennessResult(filteredUsers)

//    var userBetweennessMapRdd =sc.parallelize(userBetweennessMap)

    val writer1 = new PrintWriter(new File(outputFilePathBetweenness))

    for(betweenness <- userBetweennessMap){
      writer1.write("(\'" + betweenness._1._1 + "\', \'" + betweenness._1._2 +"\'), " + betweenness._2.toString + "\n")
    }

    writer1.close()

    //  ################################# Community Detection #############################################

    var adjacencyMatrix = generateAdjacencyMatrix(filteredUsers)

    var degreeMatrix = generateDegreeMatrix(filteredUsers)

    val m = userBetweennessMap.keySet.size

    var clusters  = generateUserClusters(filteredUsers)

    var optimizedClusters = runGirvanNewman(clusters, adjacencyMatrix, degreeMatrix, m, userBetweennessMap, filteredUsers)

    val writer = new PrintWriter(new File(outputFilePathCommunities))

    for(community <- optimizedClusters) {
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
