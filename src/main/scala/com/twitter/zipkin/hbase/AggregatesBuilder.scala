package com.twitter.zipkin.hbase

import com.twitter.zipkin.builder.Builder
import com.twitter.zipkin.storage.Aggregates
import com.twitter.zipkin.storage.hbase.HBaseAggregates
import com.twitter.zipkin.storage.hbase.utils.{ThreadProvider, HBaseTable}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

case class AggregatesBuilder(
  confOption: Option[Configuration] = Some(HBaseConfiguration.create()),
  zkServers: Option[String] = None,
  zkPort: Option[Int] = None
) extends Builder[Aggregates] with ConfBuilder {
  self =>

  def apply() = new HBaseAggregates(conf)
}
