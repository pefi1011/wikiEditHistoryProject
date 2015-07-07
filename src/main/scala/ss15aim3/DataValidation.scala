package ss15aim3

import org.apache.flink.api.common.functions.{MapFunction}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

object DataValidation {

  def main(args: Array[String]) {

    val env = ExecutionEnvironment.getExecutionEnvironment


    val editsFromWikipedia = env.readTextFile("/home/vassil/workspace/wikiEditHistoryProject/input/validationDataFileEdits.csv")
      // get only the titles of the documents
      .map(new MapFunction[String, (String, String)]() {
      def map(in: String): (String, String) = {
        val editInfors = in.split(";")
        // Remove the hours minutes and seconds of the date
        (editInfors(1), editInfors(4))
      }
    })

    val top10Catoegories = env.readTextFile("/home/vassil/workspace/wikiEditHistoryProject/output/editFileFrequency")
      .map(t => (t.split(",")(0).substring(1), t.split(",")(1).dropRight(1)))

    top10Catoegories.writeAsText("/home/vassil/top10", WriteMode.OVERWRITE)

    // Join the data
    val matches = editsFromWikipedia
      .joinWithTiny(top10Catoegories)
      .where(0)
      .equalTo(0)

    matches.writeAsText("/home/vassil/MATCHES", WriteMode.OVERWRITE)

    env.execute("Scala AssociationRule Example")
  }
}

class DataValidation {}