package ss15aim3

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

object DataValidation {

  def main(args: Array[String]) {


    if (args.length < 3) {
      sys.error("toBeComparedDataPath and toBeValidatedPath and validationResultsPath console parameters are missing")
      sys.exit(1)
    }

    var toBeComparedDataPath = args(0)
    var toBeValidatedPath = args(1)
    var validationResultsPath = args(2)
    println("toBeComparedDataPath: " + toBeComparedDataPath)
    println("toBeValidatedPath: " + toBeValidatedPath)
    println("validationResultsPath: " + validationResultsPath)


    val env = ExecutionEnvironment.getExecutionEnvironment


    val editsFromWikipedia = env.readTextFile(toBeComparedDataPath)
      // get only the titles of the documents
      .map(new MapFunction[String, (String, String)]() {
      def map(in: String): (String, String) = {
        val editInfors = in.split(";")
        // Remove the hours minutes and seconds of the date
        (editInfors(1), editInfors(4))
      }
    })

    val top10Catoegories = env.readTextFile(toBeValidatedPath)
      .map(t => (t.split(",")(0).substring(1), t.split(",")(1).dropRight(1)))

    // Join the data
    val matches = editsFromWikipedia
      .joinWithTiny(top10Catoegories)
      .where(0)
      .equalTo(0)

    matches.writeAsText(validationResultsPath, WriteMode.OVERWRITE)

    env.execute("wikipediaEditHistory validation job")
  }
}

class DataValidation {}