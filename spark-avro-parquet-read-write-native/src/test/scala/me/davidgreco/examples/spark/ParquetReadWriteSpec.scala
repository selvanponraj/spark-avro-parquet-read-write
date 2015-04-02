package me.davidgreco.examples.spark

import org.apache.avro.SchemaBuilder.FieldAssembler
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}
import parquet.avro.{AvroReadSupport, AvroParquetOutputFormat, AvroWriteSupport}
import parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}

class ParquetReadWriteSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {
  var sparkContext: SparkContext = _

  override def beforeAll() = {
    val conf = new SparkConf().
      setAppName("test-ParquetReadWriteSpec").
      setMaster("local[16]").
      set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkContext = new SparkContext(conf)
    ()
  }

  "Spark" must {
    "read and write a parquet file as generic records correctly" in {

      val input = s"file://${System.getProperty("user.dir")}/src/test/resources/test.avro"
      val output = s"file://${System.getProperty("user.dir")}/tmp/out_test.parquet"

      //I delete the output in case it exists
      val conf = new Configuration()
      val dir = new Path(output)
      val fileSystem = dir.getFileSystem(conf)
      if (fileSystem.exists(dir))
        fileSystem.delete(dir, true)

      val rdd: RDD[(Null, GenericRecord)] = sparkContext.newAPIHadoopFile[AvroKey[GenericRecord], NullWritable, AvroKeyInputFormat[GenericRecord]](input).map(p => (null, p._1.datum()))

      val fieldsAssembler: FieldAssembler[Schema] = SchemaBuilder.record("RECORD").fields()
      fieldsAssembler.name("a").`type`().nullable().intType().noDefault()
      fieldsAssembler.name("b").`type`().nullable().stringType().noDefault()
      val schema = fieldsAssembler.endRecord()

      val job = Job.getInstance(sparkContext.hadoopConfiguration)
      ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
      AvroParquetOutputFormat.setSchema(job, schema)
      import org.apache.spark.SparkContext._
      rdd.saveAsNewAPIHadoopFile(output, classOf[Void], classOf[GenericRecord], classOf[ParquetOutputFormat[GenericRecord]], job.getConfiguration)

      //now I read back the parquet file
      ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[GenericRecord]])
      val rdd2: RDD[(Void, GenericRecord)] = sparkContext.newAPIHadoopFile(output, classOf[ParquetInputFormat[GenericRecord]], classOf[Void], classOf[GenericRecord], job.getConfiguration)
      val first: (Void, GenericRecord) = rdd2.first()

      first._2.get("b") must be("CIAO0")
    }
  }

  override def afterAll() = {
    sparkContext.stop()
  }
}