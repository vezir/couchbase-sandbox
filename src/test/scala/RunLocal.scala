//import scala.tools.nsc.Main

object RunLocal {
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.master", "local[*]")
    Quickstart.main(args)
  }
}