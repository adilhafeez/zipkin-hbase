package com.twitter.zipkin.storage.hbase

import com.twitter.algebird.Monoid
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.util.{Future, Time}
import com.twitter.zipkin.common.Dependencies
import com.twitter.zipkin.conversions.thrift._
import com.twitter.zipkin.hbase.TableLayouts
import com.twitter.zipkin.storage.DependencyStore
import com.twitter.zipkin.storage.hbase.utils.HBaseTable
import com.twitter.zipkin.thriftscala
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

class HBaseDependencyStore(conf: Configuration) extends DependencyStore {

  val dependenciesTable = new HBaseTable(conf, TableLayouts.dependenciesTableName)

  val serializer = new BinaryThriftStructSerializer[thriftscala.Dependencies] {
    def codec = thriftscala.Dependencies
  }

  def close() = dependenciesTable.close()

  def getDependencies(startDate: Option[Time], endDate: Option[Time] = None): Future[Dependencies] = {
    val scan = new Scan()
    scan.setStartRow(Bytes.toBytes(Long.MaxValue - startDate.map(_.inMilliseconds).getOrElse(Long.MaxValue)))
    endDate.foreach { ed => scan.setStopRow(Bytes.toBytes(Long.MaxValue - ed.inMilliseconds)) }
    scan.addColumn(TableLayouts.dependenciesFamily, Bytes.toBytes("\0"))
    dependenciesTable.scan(scan, 100).map { results =>
      val depList = results.flatMap { result =>
        result.list().asScala.headOption.map { kv =>
          val tDep = serializer.fromBytes(kv.getValue)
          tDep.toDependencies
        }
      }
      Monoid.sum(depList)
    }
  }

  def storeDependencies(dependencies: Dependencies): Future[Unit] = {
    val rk = Bytes.toBytes(Long.MaxValue - dependencies.startTime.inMilliseconds)
    val put = new Put(rk)
    put.add(TableLayouts.dependenciesFamily, Bytes.toBytes("\0"), serializer.toBytes(dependencies.toThrift))
    dependenciesTable.put(Seq(put))
  }
}
