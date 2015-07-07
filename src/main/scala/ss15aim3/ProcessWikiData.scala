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

    env.execute("Scala AssociationRule Example")
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
}

class ProcessWikiData {}