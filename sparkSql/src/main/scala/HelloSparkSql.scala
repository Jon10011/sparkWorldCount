import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object HelloSparkSql {
  def main(args: Array[String]): Unit = {
    //获取conf
    val conf = new SparkConf().setAppName("Jon").setMaster("local[*]")

//    //获取sc
//    val sc = new SparkContext(conf)
//
//    //获取sparkSession
//    val spark = new SparkSession(sc)

    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.json()

    //生成DataFrame
    val df = spark.read.json("/Users/jon/Desktop/学习笔记/resourse/people.json")

    //展示所有数据
    df.show()

    //DSL
    df.select("name").show

    //SQL
    //创建临时表
    df.createTempView("people")

    spark.sql("select * from people").show

    //关闭
    spark.close()
    sc.stop()
  }
}
