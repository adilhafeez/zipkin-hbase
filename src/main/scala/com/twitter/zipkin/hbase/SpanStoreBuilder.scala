package com.twitter.zipkin.hbase

import com.twitter.zipkin.builder.Builder
import com.twitter.zipkin.storage.SpanStore
import com.twitter.zipkin.storage.hbase.HBaseSpanStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

/** Allows [[HBaseSpanStore]] to be used with legacy [[Builder]]s. */
case class SpanStoreBuilder(
  confOption: Option[Configuration] = Some(HBaseConfiguration.create()),
  zkServers: Option[String] = None,
  zkPort: Option[Int] = None
) extends Builder[SpanStore] with ConfBuilder {

  def apply() = new HBaseSpanStore(conf)
}
