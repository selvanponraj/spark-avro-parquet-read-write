package com.databricks.spark.avro

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.SchemaRDD

trait SchemaSupport {
  def getSchema(schemaRDD: SchemaRDD): Schema = {
    val builder = SchemaBuilder.record("topLevelRecord")
    val schema: Schema = SchemaConverters.convertStructToAvro(schemaRDD.schema, builder)
    schema
  }
}
