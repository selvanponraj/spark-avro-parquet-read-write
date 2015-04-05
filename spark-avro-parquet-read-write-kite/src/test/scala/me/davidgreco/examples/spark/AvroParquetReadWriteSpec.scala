package me.davidgreco.examples.spark

import com.databricks.spark.avro.kite._
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.kitesdk.data._
import org.kitesdk.data.mapreduce.{DatasetKeyInputFormat, DatasetKeyOutputFormat}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

case class Person(name: String, age: Int)

class AvroParquetReadWriteSpec extends WordSpec with MustMatchers with BeforeAndAfterAll with TestSupport {
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
    "read and write an avro data set using kite" in {

      cleanup()

      val descriptor = new DatasetDescriptor.Builder().schemaUri("resource:product.avsc").compressionType(CompressionType.Snappy).build() //Snappy compression is the default as the AVRO format
      val products = Datasets.create(s"dataset:file://${System.getProperty("user.dir")}/tmp/test/products", descriptor, classOf[GenericRecord]).asInstanceOf[Dataset[GenericRecord]]
      val writer = products.newWriter()
      val builder = new GenericRecordBuilder(descriptor.getSchema)
      for (i <- 1 to 100) {
        val record = builder.set("name", s"product-$i").set("id", i.toLong).build()
        writer.write(record)
      }
      writer.close()

      val job = Job.getInstance()
      DatasetKeyInputFormat.configure(job).readFrom(products).withType(classOf[GenericRecord])
      val rdd = sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[DatasetKeyInputFormat[GenericRecord]], classOf[GenericRecord], classOf[Void])
      rdd.map(p => (p._1.get("name").toString, p._1.get("id").asInstanceOf[Long])).collect() must be(
        for {
          i <- 1 to 100
        } yield (s"product-$i", i.toLong)
      )

      //now I write it back using spark and I read it again using kite
      val products2 = Datasets.create(s"dataset:file://${System.getProperty("user.dir")}/tmp/test/products2", descriptor, classOf[GenericRecord]).asInstanceOf[Dataset[GenericRecord]]
      DatasetKeyOutputFormat.configure(job).writeTo(products2)
      rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

      val reader = products2.newReader()

      import collection.JavaConversions._
      reader.iterator().toStream.map(p => (p.get("name").toString, p.get("id"))) must be(
        for {
          i <- 1 to 100
        } yield (s"product-$i", i.toLong)
      )

      cleanup()

    }
  }

  "Spark" must {
    "read and write a parquet data set using kite" in {

      cleanup()

      val descriptor = new DatasetDescriptor.Builder().schemaUri("resource:product.avsc").compressionType(CompressionType.Snappy).format(Formats.PARQUET).build() //Snappy compression is the default
      val products = Datasets.create(s"dataset:file://${System.getProperty("user.dir")}/tmp/test/products", descriptor, classOf[GenericRecord]).asInstanceOf[Dataset[GenericRecord]]
      val writer = products.newWriter()
      val builder = new GenericRecordBuilder(descriptor.getSchema)
      for (i <- 1 to 100) {
        val record = builder.set("name", s"product-$i").set("id", i.toLong).build()
        writer.write(record)
      }
      writer.close()

      val job = Job.getInstance()
      DatasetKeyInputFormat.configure(job).readFrom(products).withType(classOf[GenericRecord])
      val rdd = sparkContext.newAPIHadoopRDD(job.getConfiguration, classOf[DatasetKeyInputFormat[GenericRecord]], classOf[GenericRecord], classOf[Void])
      rdd.map(p => (p._1.get("name").toString, p._1.get("id").asInstanceOf[Long])).collect() must be(
        for {
          i <- 1 to 100
        } yield (s"product-$i", i.toLong)
      )

      //now I write it back using spark and I read it again using kite
      val products2 = Datasets.create(s"dataset:file://${System.getProperty("user.dir")}/tmp/test/products2", descriptor, classOf[GenericRecord]).asInstanceOf[Dataset[GenericRecord]]
      DatasetKeyOutputFormat.configure(job).writeTo(products2)
      rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)

      val reader = products2.newReader()

      import collection.JavaConversions._
      reader.iterator().toStream.map(p => (p.get("name").toString, p.get("id"))) must be(
        for {
          i <- 1 to 100
        } yield (s"product-$i", i.toLong)
      )

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a kite avro dataset" in {

      cleanup()

      val products = generateDataset(Formats.AVRO)

      implicit val sqlContext = new SQLContext(sparkContext)

      val data = sqlContext.kiteDatasetFile(products)

      data.registerTempTable("product")

      val res = sqlContext.sql("select * from product where id < 10")

      res.map(row => (row.getAs[String](0), row.getAs[Long](1))).collect() must be(
        for {
          i <- 1 to 9
        } yield (s"product-$i", i.toLong)
      )

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a SchemaRDD/Dataframe from a kite parquet dataset" in {

      cleanup()

      val products = generateDataset(Formats.PARQUET)

      implicit val sqlContext = new SQLContext(sparkContext)

      val data = sqlContext.kiteDatasetFile(products)

      data.registerTempTable("product")

      val res = sqlContext.sql("select * from product where id < 10")

      res.map(row => (row.getAs[String](0), row.getAs[Long](1))).collect() must be(
        for {
          i <- 1 to 9
        } yield (s"product-$i", i.toLong)
      )

      cleanup()

    }
  }

  "Spark" must {
    "be able to create a kite dataset from a SchemaRDD/Dataframe" in {

      val sqlContext = new SQLContext(sparkContext)

      import sqlContext.createSchemaRDD

      val datasetURI = URIBuilder.build(s"repo:file:////${System.getProperty("user.dir")}/tmp", "test", "persons")

      //I delete the output dir in case it exists
      val conf = new Configuration()
      val dir = new Path(s"${System.getProperty("user.dir")}/tmp")
      val fileSystem = dir.getFileSystem(conf)
      if (fileSystem.exists(dir))
        fileSystem.delete(dir, true)

      val peopleList = List(Person("David", 50), Person("Ruben", 14), Person("Giuditta", 12), Person("Vita", 19))
      val people = sparkContext.parallelize[Person](peopleList)
      people.registerTempTable("people")
      val teenagers = sqlContext.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
      val dataset = KiteDatasetSaver.saveAsKiteDataset(teenagers, datasetURI, Formats.PARQUET)

      val reader = dataset.newReader()

      import collection.JavaConversions._

      reader.iterator().toList.sortBy(g => g.get("name").toString).mkString(",") must be("{\"name\": \"Ruben\", \"age\": 14},{\"name\": \"Vita\", \"age\": 19}")
      reader.close()
    }
  }

  override def afterAll() = {
    sparkContext.stop()
  }
}