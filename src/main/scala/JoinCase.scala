import org.apache.spark.{SparkConf, SparkContext}

object JoinCase {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val words = sc.textFile("word.txt")
    val fruits = sc.textFile("fruit.txt")

    val wordsKV = words
      .flatMap(line => {
        line.split(" ")
      })
      .map(word => (word,1))

    val fruitsKV = fruits
      .map(line => {
        line.split(" ")
      })
      .map(word => (word(0),word(1)))

    wordsKV
      .reduceByKey((a,b)=>a+b)
      .join(fruitsKV)
      //.sortByKey()
      .foreach(println)

  }
}
