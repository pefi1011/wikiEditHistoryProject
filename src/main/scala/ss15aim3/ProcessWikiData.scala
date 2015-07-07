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
  private var categoryParam: String = ""

  // Get platform independent new line
  private val platfIndepNewLine = System.getProperty("line.separator")

  private val csvRowDelimeter = platfIndepNewLine
  private val newLine = platfIndepNewLine
  private val emptyLine = newLine+newLine



  def main(args: Array[String]) {

    if (args.length < 3) {
      sys.error("inputFilePath and outputPath console parameters are missing")
      sys.exit(1)
    }
    inputFilePath = args(0)
    outputFilePath = args(1)
    categoryParam = args(2)

    println("inputFilePath: " + inputFilePath)
    println("outputFilePath: " + outputFilePath)
    println("categoryParam: " + categoryParam)


    val env = ExecutionEnvironment.getExecutionEnvironment

    val edits = env.readFileOfPrimitives[String](inputFilePath, emptyLine)


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////// CATEGORIES PART ////////////////////////////////////////////

    // Assure the data has the right format (WRITE ABOUT THAT IN PAPER)
    val editsBadDataCleared = edits.filter(_.split(platfIndepNewLine)(0).split(" ").length > 5)

     val editsByCategoryType = editsBadDataCleared
       // get only data which contains "politic" in categories
       .filter(_.split(newLine)(1).toUpperCase().contains(categoryParam.toUpperCase))
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

    editsByCategoryType.writeAsText(outputFilePath + "/categoryEditTime/" + categoryParam, WriteMode.OVERWRITE)

    env.execute("Scala AssociationRule Example")
  }
}

class ProcessWikiData {}