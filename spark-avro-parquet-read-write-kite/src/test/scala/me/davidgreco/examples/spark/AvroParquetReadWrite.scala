package me.davidgreco.examples.spark

import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.kitesdk.data._
import org.kitesdk.data.mapreduce.{DatasetKeyInputFormat, DatasetKeyOutputFormat}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

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
    "read and write an avro data set using kite" in {

      //I delete the output in case it exists
      val conf = new Configuration()
      val dir = new Path(s"${System.getProperty("user.dir")}/tmp/")
      val fileSystem = dir.getFileSystem(conf)
      if (fileSystem.exists(dir))
        fileSystem.delete(dir, true)

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
    }
  }


  "Spark" must {
    "read and write a parquet data set using kite" in {

      //I delete the output in case it exists
      val conf = new Configuration()
      val dir = new Path(s"${System.getProperty("user.dir")}/tmp/")
      val fileSystem = dir.getFileSystem(conf)
      if (fileSystem.exists(dir))
        fileSystem.delete(dir, true)

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
   }
  }

  override def afterAll() = {
    sparkContext.stop()
  }
}