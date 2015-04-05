package com.databricks.spark.avro

import java.net.URI

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SchemaRDD
import org.kitesdk.data._
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat

package object kite {

  implicit class KiteDatasetContext(sqlContext: org.apache.spark.sql.SQLContext) {
    def kiteDatasetFile(dataSet: Dataset[_], minPartitions: Int = 0): org.apache.spark.sql.SchemaRDD = {
      import com.databricks.spark.avro._
      dataSet.getDescriptor.getFormat match {
        case Formats.AVRO => sqlContext.avroFile(dataSet.getDescriptor.getLocation.getRawPath, minPartitions)
        case Formats.PARQUET => sqlContext.parquetFile(dataSet.getDescriptor.getLocation.getRawPath)
      }
    }
  }

  object KiteDatasetSaver extends SchemaSupport {
    def saveAsKiteDataset(schemaRDD: SchemaRDD, uri: URI, format: Format = Formats.AVRO): Dataset[GenericRecord] = {
      assert(URIBuilder.DATASET_SCHEME == uri.getScheme, s"Not a dataset or view URI: $uri" + "")

      val schema = getSchema(schemaRDD)
      val descriptor = new DatasetDescriptor.Builder().schema(schema).format(format).build() //Snappy compression is the default as the AVRO format
      val job = Job.getInstance()
      val dataset = Datasets.create(uri, descriptor, classOf[GenericRecord]).asInstanceOf[Dataset[GenericRecord]]
      DatasetKeyOutputFormat.configure(job).writeTo(dataset)
      schemaRDD.mapPartitions(rows => {
        val converter = AvroSaver.createConverter(schemaRDD.schema, "topLevelRecord")
        rows.map(x => (converter(x).asInstanceOf[GenericRecord],
          null))
      }).saveAsNewAPIHadoopDataset(job.getConfiguration)
      dataset
    }
  }


}
