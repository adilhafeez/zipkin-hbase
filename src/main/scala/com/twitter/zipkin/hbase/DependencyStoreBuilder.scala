package com.twitter.zipkin.hbase

import com.twitter.zipkin.builder.Builder
import com.twitter.zipkin.storage.DependencyStore
import com.twitter.zipkin.storage.hbase.HBaseDependencyStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

case class DependencyStoreBuilder(
  confOption: Option[Configuration] = Some(HBaseConfiguration.create()),
  zkServers: Option[String] = None,
  zkPort: Option[Int] = None
) extends Builder[DependencyStore] with ConfBuilder {
  self =>

  def apply() = new HBaseDependencyStore(conf)
}
