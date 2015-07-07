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



    // Assure the data has the right format (WRITE ABOUT THAT IN PAPER)
    val editsBadDataCleared = edits.filter(_.split(platfIndepNewLine)(0).split(" ").length > 5)

    val editsFirstLine = editsBadDataCleared.map(t => t.split(platfIndepNewLine)(0))
    //val editsFirstLineNoAnonym = editsFirstLine.filter(!_.split(" ")(5).startsWith("ip:"))

    val authors = editsFirstLine.map(t => t.split(" ")(5))

    val countsEditsPerUser = authors
      // Filter the anonymous users
      .filter(!_.startsWith("ip:"))
      .filter(!_.contains("Bot"))
      .filter(!_.contains("bot"))
      .map(edit => (edit, 1)).groupBy(0)
      .reduce((t1, t2) => (t1._1, t1._2 + t2._2))

    val top10Users = countsEditsPerUser
      .sortPartition(1, Order.DESCENDING)
      .setParallelism(1)
      .first(20)

    top10Users.writeAsText(outputFilePath + "/top10UsersByCount", WriteMode.OVERWRITE)

    env.execute("Scala AssociationRule Example")
  }

}

class ProcessWikiData {}