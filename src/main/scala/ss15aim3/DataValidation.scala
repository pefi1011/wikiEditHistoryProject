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
    var toBeComparedDataPath = args(0) // "/home/vassil/workspace/wikiEditHistoryProject/input/validationDataFileEdits.csv"
    var toBeValidatedPath = args(1) // "/home/vassil/workspace/wikiEditHistoryProject/output/editFileFrequency"
    var validationResultsPath = args(2) // /home/vassil/workspace/wikiEditHistoryProject/output/validationResults/
    println("toBeComparedDataPath: " + toBeComparedDataPath)
    println("toBeValidatedPath: " + toBeValidatedPath)
    println("validationResultsPath: " + validationResultsPath)


    val env = ExecutionEnvironment.getExecutionEnvironment

    //////////////////////////// START EDITS BY DOC ///////////////////////////////////////////

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
    val matchesDocs = editsFromWikipedia
      .joinWithTiny(top10Catoegories)
      .where(0)
      .equalTo(0)

    matchesDocs.writeAsText(validationResultsPath, WriteMode.OVERWRITE)

    ////////////////////////////// END EDITS BY USER ////////////////////////////////////////////


    //////////////////////////// START EDITS BY USER ////////////////////////////////////////////

    val editorsFromWikipedia = env.readTextFile("/home/vassil/workspace/wikiEditHistoryProject/input/validationDataEditsPerUser.csv")
      // bocause of some data problem :X
      .filter(_.split((";")).size >2)
      .map(t => (t.split(";")(1), t.split(";")(2)))

    val top10Editors = env.readTextFile("/home/vassil/workspace/wikiEditHistoryProject/output/top10UsersByCount")
      .map(t => (t.split(",")(0).substring(1), t.split(",")(1).dropRight(1)))

    // Join the data
    val matchesEditors = top10Editors
      .joinWithTiny(editorsFromWikipedia)
      .where(0)
      .equalTo(0)

    top10Editors.writeAsText("/home/vassil/1", WriteMode.OVERWRITE)

    editorsFromWikipedia.writeAsText("/home/vassil/2", WriteMode.OVERWRITE)

    matchesEditors.writeAsText("/home/vassil/testEditors", WriteMode.OVERWRITE)


    ////////////////////////////// END EDITS BY USER ////////////////////////////////////////////


    env.execute("wikipediaEditHistory validation job")
  }
}

class DataValidation {}