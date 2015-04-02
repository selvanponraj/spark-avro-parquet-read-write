package me.davidgreco.examples.spark

import org.apache.avro.SchemaBuilder.FieldAssembler
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroKeyInputFormat, AvroKeyOutputFormat}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}


class AvroReadWriteSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {
  var sparkContext: SparkContext = _

  override def beforeAll() = {
    val conf = new SparkConf().
      setAppName("test-AvroReadWriteSpec").
      setMaster("local[16]")
    sparkContext = new SparkContext(conf)
    ()
  }

  "Spark" must {
    "load an avro file as generic records correctly" in {

      val input = s"file://${System.getProperty("user.dir")}/src/test/resources/test.avro"

      val rdd: RDD[(AvroKey[GenericRecord], NullWritable)] = sparkContext.newAPIHadoopFile[AvroKey[GenericRecord], NullWritable, AvroKeyInputFormat[GenericRecord]](input)

      val rows: RDD[String] = rdd.map(gr => gr._1.datum().get("b").toString)

      rows.first() must be("CIAO0")
    }
  }

  "Spark" must {
    "write an avro file as generic records correctly" in {

      val input = s"file://${System.getProperty("user.dir")}/src/test/resources/test.avro"
      val output = s"file://${System.getProperty("user.dir")}/tmp/out_test.avro"

      //I delete the output in case it exists
      val conf = new Configuration()
      val dir = new Path(output)
      val fileSystem = dir.getFileSystem(conf)
      if (fileSystem.exists(dir))
        fileSystem.delete(dir, true)

      val fieldsAssembler: FieldAssembler[Schema] = SchemaBuilder.record("RECORD").fields()
      fieldsAssembler.name("a").`type`().nullable().intType().noDefault()
      fieldsAssembler.name("b").`type`().nullable().stringType().noDefault()
      val schema = fieldsAssembler.endRecord()
      val rdd: RDD[(AvroKey[GenericRecord], NullWritable)] = sparkContext.newAPIHadoopFile[AvroKey[GenericRecord], NullWritable, AvroKeyInputFormat[GenericRecord]](input)

      import org.apache.spark.SparkContext._
      sparkContext.hadoopConfiguration.set("avro.schema.output.key", schema.toString)
      rdd.saveAsNewAPIHadoopFile[AvroKeyOutputFormat[GenericRecord]](output)
      val rdd2: RDD[(AvroKey[GenericRecord], NullWritable)] = sparkContext.newAPIHadoopFile[AvroKey[GenericRecord], NullWritable, AvroKeyInputFormat[GenericRecord]](output)
      val rows: RDD[String] = rdd2.map(gr => gr._1.datum().get("b").toString)

      rows.first() must be("CIAO0")
    }
  }

  override def afterAll() = {
    sparkContext.stop()
  }
}