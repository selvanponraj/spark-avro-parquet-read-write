package me.davidgreco.examples.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{MustMatchers, BeforeAndAfterAll, WordSpec}

class AvroParquetReadWrite extends WordSpec with MustMatchers with BeforeAndAfterAll {
  var sparkContext: SparkContext = _

  override def beforeAll() = {
    val conf = new SparkConf().
      setAppName("spark-cdh5-template-local-test").
      setMaster("local[16]").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkContext = new SparkContext(conf)
    ()
  }

  "Spark" must {
    "load an avro file as a schema rdd correctly" in {

      val sqlContext = new SQLContext(sparkContext)

      val input = s"file://${System.getProperty("user.dir")}/src/test/resources/test.avro"

      import com.databricks.spark.avro._

      val data = sqlContext.avroFile(input)

      data.registerTempTable("test")

      val res = sqlContext.sql("select * from test where a < 10")

      res.collect().toList.toString must be("List([0,CIAO0], [1,CIAO1], [2,CIAO2], [3,CIAO3], [4,CIAO4], [5,CIAO5], [6,CIAO6], [7,CIAO7], [8,CIAO8], [9,CIAO9])")
    }
  }


  "Spark" must {
    "read and write a parquet file as a schema rdd correctly" in {

      val input = s"file://${System.getProperty("user.dir")}/src/test/resources/test.avro"
      val output = s"file://${System.getProperty("user.dir")}/tmp/out_test.parquet"

      //I delete the output in case it exists
      val conf = new Configuration()
      val dir = new Path(output)
      val fileSystem = dir.getFileSystem(conf)
      if (fileSystem.exists(dir))
        fileSystem.delete(dir, true)

      val sqlContext = new SQLContext(sparkContext)

      import com.databricks.spark.avro._

      val data = sqlContext.avroFile(input)

      data.registerTempTable("test")

      val res = sqlContext.sql("select * from test where a < 10")

      res.saveAsParquetFile(output)

      val rdd2 = sqlContext.parquetFile(output)
      res.collect().toList.toString must be("List([0,CIAO0], [1,CIAO1], [2,CIAO2], [3,CIAO3], [4,CIAO4], [5,CIAO5], [6,CIAO6], [7,CIAO7], [8,CIAO8], [9,CIAO9])")
    }
  }

  override def afterAll() = {
    sparkContext.stop()
  }
}