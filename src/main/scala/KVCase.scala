import org.apache.spark.{SparkConf, SparkContext}

object KVCase {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val words = sc.textFile("word.txt")


    val wordsKV = words
      .flatMap(line => {
        line.split(" ")
      })
      .map(word => (word,1))
      .cache()

    wordsKV
      .reduceByKey((a,b)=>a+b)
      .foreach(println)

    wordsKV
      .groupByKey()
      .foreach(println)

  }
}
