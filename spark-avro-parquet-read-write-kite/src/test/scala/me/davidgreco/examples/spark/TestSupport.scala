package me.davidgreco.examples.spark

import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext
import org.kitesdk.data._

trait TestSupport {
  protected def cleanup(): Unit = {
    val conf = new Configuration()
    val dir = new Path(s"${System.getProperty("user.dir")}/tmp/")
    val fileSystem = dir.getFileSystem(conf)
    if (fileSystem.exists(dir))
      fileSystem.delete(dir, true)
    ()
  }

  protected def generateDataset(format: Format) = {
    val descriptor = new DatasetDescriptor.Builder().schemaUri("resource:product.avsc").compressionType(CompressionType.Snappy).format(format).build() //Snappy compression is the default
    val products = Datasets.create(s"dataset:file://${System.getProperty("user.dir")}/tmp/test/products", descriptor, classOf[GenericRecord]).asInstanceOf[Dataset[GenericRecord]]
    val writer = products.newWriter()
    val builder = new GenericRecordBuilder(descriptor.getSchema)
    for (i <- 1 to 100) {
      val record = builder.set("name", s"product-$i").set("id", i.toLong).build()
      writer.write(record)
    }
    writer.close()
    products
  }

  protected def kiteDataset2RDD(dataSet: Dataset[_])(implicit sqlContext: SQLContext) = {
    import com.databricks.spark.avro._
    dataSet.getDescriptor.getFormat match {
      case Formats.AVRO => sqlContext.avroFile(dataSet.getDescriptor.getLocation.getRawPath)
      case Formats.PARQUET => sqlContext.parquetFile(dataSet.getDescriptor.getLocation.getRawPath)
    }
  }

}
