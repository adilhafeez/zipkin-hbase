package com.twitter.zipkin.storage.hbase

import com.twitter.algebird.Moments
import com.twitter.util.{Await, Time}
import com.twitter.zipkin.common.{Dependencies, DependencyLink, Service}
import com.twitter.zipkin.hbase.{DependencyStoreBuilder, TableLayouts}
import com.twitter.zipkin.storage.hbase.utils.HBaseTable
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

class HBaseHBaseDependencyStoreSpec extends ZipkinHBaseSpecification {

  val tablesNeeded = Seq(
    TableLayouts.dependenciesTableName,
    TableLayouts.idGenTableName,
    TableLayouts.mappingTableName
  )

  val dependencyStore = DependencyStoreBuilder(confOption = Some(_conf))()

  val m1 = Moments(1)
  val m2 = Moments(2)
  val d1 = DependencyLink(Service("HBase.Client"), Service("HBase.RegionServer"), m1)
  val d2 = DependencyLink(Service("HBase.Master"), Service("HBase.RegionServer"), m2)
  val deps = Dependencies(Time.fromSeconds(2), Time.fromSeconds(1000), List(d1, d2))

  test("storeDependencies") {
    Await.result(dependencyStore.storeDependencies(deps))
    val depsTable = new HBaseTable(_conf, TableLayouts.dependenciesTableName)
    val get = new Get(Bytes.toBytes(Long.MaxValue - Time.fromSeconds(2).inMilliseconds))
    val result = Await.result(depsTable.get(Seq(get)))
    result.size should be (1)
  }

  test("getDependencies") {
    Await.result(dependencyStore.storeDependencies(deps))
    val retrieved = Await.result(dependencyStore.getDependencies(Some(Time.fromSeconds(100))))
    retrieved should be (deps)
  }
}
