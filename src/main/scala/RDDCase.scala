import org.apache.spark.{SparkConf, SparkContext}

object RDDCase {
  def main(args: Array[String]) {
    val inputFile =  "fruit.txt"
    val conf = new SparkConf().setAppName("fruit").setMaster("local")
    val sc = new SparkContext(conf)
    val fruits = sc.textFile(inputFile)

    fruits.foreach(println)

    fruits.filter(line => {
      line.contains("apple")
    }).foreach(println)


    fruits.filter(line => {
      line.contains("apple")
    }).map(line => {
      line.replace(" ", ",")
    }).foreach(println)

  }
}