package ss15aim3

import java.io._

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector


object ProcessWikiData {

  val fileInput: Boolean = true


  // private val inputFilePath: String = "/home/vassil/workspace/inputOutput/input/aim/input"
  // private val outputFilePath: String = "/home/vassil/workspace/inputOutput/output/aim/"

  //private val inputFilePath: String = "/Software/Workspace/vslGithub/flink/flink-examples/flink-scala-examples/src/main/scala/org/apache/flink/examples/scala/recomendation/input"
  // private val outputFilePath: String = "/Software/Workspace/vslGithub/inputOutput/output/aim/"

  private var inputFilePath: String = ""
  private var outputFilePath: String = ""

  // Get platform independent new line
  private val platfIndepNewLine = System.getProperty("line.separator")

  private val csvRowDelimeter = platfIndepNewLine
  private val newLine = platfIndepNewLine
  private val emptyLine = newLine+newLine


  private val csvFieldDelimeter = ";"

  def main(args: Array[String]) {

    if (args.length < 2) {
      sys.error("inputFilePath and outputPath console parameters are missing")
      sys.exit(1)
    }
    inputFilePath = args(0)
    outputFilePath = args(1)
    println("inputFilePath: " + inputFilePath)
    println("outputFilePath: " + outputFilePath)


    val env = ExecutionEnvironment.getExecutionEnvironment

    val edits = env.readFileOfPrimitives[String](inputFilePath, emptyLine)


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////// CATEGORIES PART ////////////////////////////////////////////

    // Assure the data has the right format (WRITE ABOUT THAT IN PAPER)
    val editsBadDataCleared = edits.filter(_.split(platfIndepNewLine)(0).split(" ").length > 5)

    val userCategoryTuples = createUserCategoryTuples(editsBadDataCleared)
      // remove duplicate categories for user
      .distinct(0, 1)

    // Use that if you need all categories of user
    /*
    val allCategoriesForUser = userCategoryTuples
      // Group by user
      .groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + " " + t2._2))
    */

    // We don't need to write that
    //allCategoriesForUser.writeAsText(outputFilePath + "/categoriesPerEditor", WriteMode.OVERWRITE)

    val countCategoriesPerEditor = userCategoryTuples
      .map(userWithCategory => (userWithCategory._1, 1))
      .groupBy(0)
      .sum(1)

    val countAuthors = countCategoriesPerEditor.map(t => 1).reduce(_+_)

    val partAuthorsWith3orLessCtgs = countCategoriesPerEditor
      // get authors with 3 or less categories
      .filter(_._2 <= 3)
      // count them
      .map(t => 1).reduce(_ + _)
      // cross with authors count
      .crossWithTiny(countAuthors)
      // calculate result
      .map(t => (t._1.toDouble / t._2, 1 - (t._1.toDouble / t._2)))


    partAuthorsWith3orLessCtgs.writeAsText(outputFilePath + "/editUsersByCategory", WriteMode.OVERWRITE)



    val categoriesNotSorted = userCategoryTuples
      // get only the categories
      .map(t => (t._2, 1))
      // group by categories
      .groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))

    val categories = categoriesNotSorted
      .sortPartition(1, Order.DESCENDING)
      .setParallelism(1)
      .first(20)

    categories.writeAsText(outputFilePath + "/editFileFrequencyPart3", WriteMode.OVERWRITE)

    //////////////////////////////////////////// END CATEGORIES ///////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    val editsFirstLine = editsBadDataCleared.map(t => t.split(platfIndepNewLine)(0))
    val editsFirstLineNoAnonym = editsFirstLine.filter(!_.split(" ")(5).startsWith("ip:"))


    //////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////////////// DOCID PART //////////////////////////////////////////////////

    val editsByDocIdNotSorted = editsFirstLine //NoAnonym
      // get all doc titles
      .map(t => (t.split(" ")(3), 1))
      .groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))

    val editsByDocId = editsByDocIdNotSorted
      .sortPartition(1, Order.DESCENDING)
      .setParallelism(1)
      .first(20)

    editsByDocId.writeAsText(outputFilePath + "/editFileFrequency", WriteMode.OVERWRITE)



    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    val docsWithCategories = editsBadDataCleared.map( new MapFunction[String, (String, List[String])] {
      override def map(t: String): (String, List[String]) = {
        val infos = t.split(newLine)

        val docName = infos(0).split(" ")(3)
        val categories : List[String] = infos(1).replace("CATEGORY", "").split(" ").toList

        (docName, categories)
      }
    })
    .groupBy(0)
    .reduce( (t1, t2) => (t1._1, (t1._2 ++ t2._2).distinct))

    val catsOfFirst20Docs = docsWithCategories
      .joinWithTiny(editsByDocId)
      .where(0)
      .equalTo(0)
      .map(t => (t._1._1, t._1._2))

    val countCatsOfTop20Docs = catsOfFirst20Docs.flatMap(_._2)
      .map(t => (t , 1))
      .groupBy(0)
      .sum(1)

    countCatsOfTop20Docs.writeAsText(outputFilePath + "/editFileFrequencyPart2", WriteMode.OVERWRITE)
    //////////////////////////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////// END DOCID PART ///////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////


    ////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////// START TIMESTAMP PART /////////////////////////////////////

    generateDataByDate(editsFirstLineNoAnonym)

    // TODO join with categories or just use data samehow
    val toJoin = categories

     val editsByCategoryType = editsBadDataCleared.
       // get only data which contains "politic" in categories
       filter(_.split(newLine)(1).contains("politic"))
       .map(
         new MapFunction[String, (String, Int)]() {

           def map(in: String): (String, Int) = {

             val firstLine = in.split(newLine)(0)
             val timestamp = firstLine.split(" ")(4)
             val timeIndex = timestamp.indexOf("T")

             // Remove the hours minutes and seconds of the date
             (timestamp.dropRight(timeIndex), 1)
           }
         }
       )
       .groupBy(0)
       .sum(1)

    editsByCategoryType.writeAsText(outputFilePath + "/categoryEditTime/politics", WriteMode.OVERWRITE)



    ///////////////////////////// END TIMESTAMP PART ///////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////


    val authors = editsFirstLine.map(t => t.split(" ")(5))

    val countsEditsPerUser = authors
      // Filter the anonymous users
      .filter(!_.startsWith("ip:"))
      .map(edit => (edit, 1)).groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))

    val top10Users = countsEditsPerUser
      .sortPartition(1, Order.DESCENDING)
      .setParallelism(1)
      .first(20)

    top10Users.writeAsText(outputFilePath + "/top10UsersByCount", WriteMode.OVERWRITE)

    val countUsers = countsEditsPerUser.map(_ => 1).reduce(_+_)

    val sumEditsByUser = countsEditsPerUser
      .map(edit => edit._2)
      .reduce(_ + _)

    val avgEditsCountPerUser = countUsers
      .crossWithTiny(sumEditsByUser)
      // edits / countUsers
      .map(in => in._2.toDouble / in._1)

    val editsByAnonymous = authors
      .filter(_.startsWith("ip:"))
      // Get the count
      .map(_ => 1).reduce(_ + _)

    val editsTotal = authors
      .map(_ => 1).reduce(_ + _)

    val percentageAnonymous = editsByAnonymous
      .cross(editsTotal)
      .map(in => (2015, in._1.toDouble / in._2, 1 - (in._1.toDouble / in._2)))

    percentageAnonymous.writeAsCsv(outputFilePath + "/editionByAnonyms", csvRowDelimeter, csvFieldDelimeter, WriteMode.OVERWRITE)
    //countsEditsPerUser.writeAsText(outputFilePath + "/editsPerUser", WriteMode.OVERWRITE)


    val countPerUserGroup = calculateUserGroups(countsEditsPerUser)

    val outputCountPerUserGroupAndAvg = countPerUserGroup
      .crossWithTiny(avgEditsCountPerUser)
      .map(t => (2015, t._1._1, t._1._2, t._1._3, t._1._4, t._1._5, t._1._6, t._1._7, t._2))

    outputCountPerUserGroupAndAvg.writeAsCsv(outputFilePath + "/editsUsersByDocCount", csvRowDelimeter, csvFieldDelimeter, WriteMode.OVERWRITE)

    env.execute("Scala AssociationRule Example")
  }

  private def parseText(textInput: DataSet[String]) = {

    textInput.flatMap { input =>
      input.split(emptyLine)
    }
  }

  def writeInCsv(tuples: DataSet[(String, Int)], pfad: String)() = {
    var names: String = "YEAR;"
    for (tuple <- tuples.collect()) {
      names = names + tuple._1 + ";"
    }

    var counts: String = "2015;"
    for (tuple <- tuples.collect()) {
      counts = counts + tuple._2 + ";"
    }

    val pw = new PrintWriter(new File(pfad))
    pw.write(names.dropRight(1) + platfIndepNewLine)
    pw.write(counts.dropRight(1))
    pw.close()
  }

  def createUserCategoryTuples(editsBadDataCleared: DataSet[String]): DataSet[(String, String)] = {

    // Get author, category tuples
    editsBadDataCleared.flatMap(new FlatMapFunction[String, (String, String)]() {

      def flatMap(in: String, out: Collector[(String, String)]) = {

        // get the single rows of the data
        val rows = in.split(platfIndepNewLine)

        // get the author of the edit which is in the first row on the 6th position
        val editor = rows(0).split(" ")(5)

        // get the categories
        val categories = rows(1).split(" ") //.drop(1)
        //for each category create a new tuple author category

        var isFirst: Boolean = true
        for (cat <- categories) {
          if (isFirst) {
            isFirst = false
          } else {
            out.collect((editor, cat))
          }
        }
      }
    })
  }


  def calculateUserGroups(countsEditsPerUser: DataSet[(String, Int)]): DataSet[(Int, Int, Int, Int, Int, Int, Int)] = {

    val countPerUserGroup = countsEditsPerUser.map(new MapFunction[(String, Int), (Int, Int, Int, Int, Int, Int, Int)]() {
      def map(in: (String, Int)): (Int, Int, Int, Int, Int, Int, Int) = {
        var isIn0_5Group = 0
        var isIn6_10Group = 0
        var isIn11_15Group = 0
        var isIn16_20Group = 0
        var isIn21_25Group = 0
        var isIn26_30Group = 0
        var isIn30PGroup = 0
        if (in._2 <= 5) {
          isIn0_5Group = 1
        } else if (in._2 <= 10) {
          isIn6_10Group = 1
        } else if (in._2 <= 15) {
          isIn11_15Group = 1
        } else if (in._2 <= 20) {
          isIn16_20Group = 1
        } else if (in._2 <= 25) {
          isIn21_25Group = 1
        } else if (in._2 <= 30) {
          isIn26_30Group = 1
        } else {
          isIn30PGroup = 1
        }
        (isIn0_5Group, isIn6_10Group, isIn11_15Group, isIn16_20Group, isIn21_25Group, isIn26_30Group, isIn30PGroup)
      }
    })
      // calculate the count in each group
      .reduce((t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3, t1._4 + t2._4, t1._5 + t2._5, t1._6 + t2._6, t1._7 + t2._7))

    countPerUserGroup

  }

  private def getTextDataSet(env: ExecutionEnvironment): DataSet[String] = {

    if (fileInput) {
      println("From File")
      env.readTextFile(inputFilePath)
    }

    else {
      println("From Code")
      env.fromCollection(RecommendationData.ITEMS)
    }
  }

  def generateDataByDate(editsFirstLine: DataSet[String]) = {

    val userEditsByDate = editsFirstLine
      // get all edit timestamps
      .map(

        new MapFunction[String, (String, String, Int)]() {
          def map(in: String): (String, String, Int) = {

            val user = in.split(" ")(5)
            val timestamp = in.split(" ")(4)
            val timeIndex = timestamp.indexOf("T")

            // Remove the hours minutes and seconds of the date
            (timestamp.dropRight(timeIndex), user, 1)
          }
        })


    val countEditsByDate = userEditsByDate
      .map(t => (t._1, t._3))
      .groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))

    countEditsByDate.writeAsCsv(outputFilePath + "/userEditTimeCount", csvRowDelimeter, csvFieldDelimeter, WriteMode.OVERWRITE)

    val countEditsByUserDate = userEditsByDate
      .groupBy(0, 1)
      .reduce((t1, t2) => (t1._1, t1._2, t1._3 + t2._3))

    countEditsByUserDate.writeAsCsv(outputFilePath + "/userEditTime", csvRowDelimeter, csvFieldDelimeter, WriteMode.OVERWRITE)

  }


}

class ProcessWikiData {}