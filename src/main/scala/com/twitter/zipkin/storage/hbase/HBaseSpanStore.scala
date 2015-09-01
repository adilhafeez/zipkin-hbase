package com.twitter.zipkin.storage.hbase

import java.nio.ByteBuffer

import com.twitter.logging.Logger
import com.twitter.scrooge.BinaryThriftStructSerializer
import com.twitter.util.Future
import com.twitter.zipkin.common.Span
import com.twitter.zipkin.conversions.thrift._
import com.twitter.zipkin.hbase.TableLayouts
import com.twitter.zipkin.storage.hbase.mapping.ServiceMapper
import com.twitter.zipkin.storage.hbase.utils.{HBaseTable, IDGenerator, ThreadProvider}
import com.twitter.zipkin.storage.{IndexedTraceId, SpanStore}
import com.twitter.zipkin.util.Util
import com.twitter.zipkin.{Constants, thriftscala}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Get, Put, Result, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, ValueFilter}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._

/**
 * Storage to store spans into an HBase Table. TTL is handled by HBase.
 *
 * The HBase table is laid out as follows:
 *
 * RowKey: [ TraceId ]
 * Column Family: D
 * Column Qualifier: [ SpanId ][ Hash of Annotations ]
 * Column Value: Thrift Serialized Span
 */
class HBaseSpanStore(conf: Configuration) extends SpanStore {
  private[this] val hbaseTable = new HBaseTable(conf, TableLayouts.storageTableName, mainExecutor = ThreadProvider.storageExecutor)

  val log = Logger.get(getClass.getName)

  private[this] val idxServiceTable = new HBaseTable(conf, TableLayouts.idxServiceTableName, mainExecutor = ThreadProvider.indexServiceExecutor)
  private[this] val idxServiceSpanNameTable = new HBaseTable(conf, TableLayouts.idxServiceSpanNameTableName, mainExecutor = ThreadProvider.indexServiceSpanExecutor)
  private[this] val idxServiceAnnotationTable = new HBaseTable(conf, TableLayouts.idxServiceAnnotationTableName, mainExecutor = ThreadProvider.indexAnnotationExecutor)

  private[this] val mappingTable = new HBaseTable(conf, TableLayouts.mappingTableName, mainExecutor = ThreadProvider.mappingTableExecutor)
  private[this] val idGenTable = new HBaseTable(conf, TableLayouts.idGenTableName, mainExecutor = ThreadProvider.idGenTableExecutor)

  private[this] lazy val idGen = new IDGenerator(idGenTable)
  private[this] lazy val serviceMapper = new ServiceMapper(mappingTable, idGen)
  private[this] val serializer = new BinaryThriftStructSerializer[thriftscala.Span] {
    def codec = thriftscala.Span
  }

  /**
   * Get the trace ids for this particular service and if provided, span name.
   * Only return maximum of limit trace ids from before the endTs.
   */
  override def getTraceIdsByName(serviceName: String, spanNameOption: Option[String], endTs: Long, limit: Int): Future[Seq[IndexedTraceId]] = {
    val resultsFuture = spanNameOption match {
      case None       => getTraceIdsByNameNoSpanName(serviceName, endTs, limit)
      case Some(spanName) => getTraceIdsByNameWithSpanName(serviceName, spanName, endTs, limit)
    }

    resultsFuture.map { results =>
      results.flatMap { result => indexResultToTraceId(result) }.distinct.take(limit)
    }
  }

  /**
   * Get the trace ids for this annotation between the two timestamps. If value is also passed we expect
   * both the annotation key and value to be present in index for a match to be returned.
   * Only return maximum of limit trace ids from before the endTs.
   */
  override def getTraceIdsByAnnotation(serviceName: String, annotation: String, value: Option[ByteBuffer], endTs: Long, limit: Int): Future[Seq[IndexedTraceId]] = {
    val serviceMappingFuture = serviceMapper.get(serviceName)
    val annoMappingFuture = serviceMappingFuture.flatMap { serviceMapping =>
      serviceMapping.annotationMapper.get(annotation)
    }

    annoMappingFuture.flatMap { annoMapping =>
      val scan = new Scan()
      val startRk = Bytes.toBytes(annoMapping.parent.get.id) ++ Bytes.toBytes(annoMapping.id) ++ getEndScanTimeStampRowKeyBytes(endTs)
      val endRk = Bytes.toBytes(annoMapping.parent.get.id) ++ Bytes.toBytes(annoMapping.id) ++ Bytes.toBytes(Long.MaxValue)
      scan.setStartRow(startRk)
      scan.setStopRow(endRk)
      scan.addFamily(TableLayouts.idxAnnotationFamily)
      value.foreach { bb => scan.setFilter(new ValueFilter(CompareOp.EQUAL, new BinaryComparator(Util.getArrayFromBuffer(bb)))) }
      idxServiceAnnotationTable.scan(scan, limit).map { results =>
        results.flatMap { result => indexResultToTraceId(result)}.distinct.take(limit)
      }
    }
  }

  /**
   * Get all the service names for as far back as the ttl allows.
   */
  override def getAllServiceNames: Future[Set[String]] = serviceMapper.getAll.map { f => f.map(_.name) }

  /**
   * Get all the span names for a particular service, as far back as the ttl allows.
   */
  override def getSpanNames(service: String): Future[Set[String]] = {
    // From the service get the spanNameMapper.  Then get all the maps.
    val spanNameMappingsFuture = serviceMapper.get(service).flatMap { _.spanNameMapper.getAll }
    // get the names from the mappings.
    spanNameMappingsFuture.map { maps => maps.map { _.name} }
  }

  /**
   * Store the span in the underlying storage for later retrieval.
   * @return a future for the operation
   */
  override def apply(spans: Seq[Span]): Future[Unit] = {
    hbaseTable.put(spans.map(span => {
      val rk = rowKeyFromSpan(span)
      val p = new Put(rk)
      val qual = Bytes.toBytes(span.id) ++ Bytes.toBytes(span.annotations.hashCode())
      p.add(TableLayouts.storageFamily, qual, serializer.toBytes(span.toThrift))
    })).flatMap(done => {
      Future.collect(spans.flatMap {
        span =>
          Seq(
            indexServiceName(span),
            indexSpanNameByService(span),
            indexTraceIdByServiceAndName(span),
            indexSpanByAnnotations(span))
      }).unit
    })
  }

  /**
   * Index a trace id on the service and name of a specific Span
   */
  private[hbase] def indexTraceIdByServiceAndName(span: Span): Future[Unit] = {
    // Get the id of services and span names
    val serviceMappingsFuture =  Future.collect( span.serviceNames.map { sn =>
      serviceMapper.get(sn)
    }.toSeq)

    // Figure out when this happened.
    val timeBytes = getTimeStampRowKeyBytes(span)

    val traceIdBytes = Bytes.toBytes(span.traceId)
    val putsFuture = serviceMappingsFuture.flatMap { serviceMappings =>
      Future.collect(serviceMappings.map { serviceMapping =>
        val putF: Future[Put] = serviceMapping.spanNameMapper.get(span.name).map { spanNameMapping =>
          val rk = Bytes.toBytes(serviceMapping.id) ++ Bytes.toBytes(spanNameMapping.id) ++ timeBytes
          val p = new Put(rk)
          p.add(TableLayouts.idxServiceSpanNameFamily, traceIdBytes, Bytes.toBytes(true))
          p
        }
        putF
      })
    }

    // Put the data into hbase.
    putsFuture.flatMap { puts => idxServiceSpanNameTable.put(puts) }
  }

  /**
   * Index the span by the annotations attached
   */
  private[hbase] def indexSpanByAnnotations(span: Span): Future[Unit] = {
    // Get the normal annotations
    val annoFutures = span.annotations.filter { a =>
      // skip core annotations since that query can be done by service name/span name anyway 5
      !Constants.CoreAnnotations.contains(a.value)
    }.map { a =>
      val sf = serviceMapper.get(a.serviceName)
      sf.flatMap { service => service.annotationMapper.get(a.value)}.map { am => (am, Bytes.toBytes(true))}
    }

    // Get the binary annotations.
    val baFutures = span.binaryAnnotations.map { ba =>
      ba.host match {
        case Some(host) => Some((ba, host))
        case None => None
      }
    }.flatten.map { case (ba, host) =>
      val sf = serviceMapper.get(host.serviceName)
      sf.flatMap { service =>
        service.annotationMapper.get(ba.key)
      }.map { am =>
        val bytes = Util.getArrayFromBuffer(ba.value)
        (am, bytes)
      }
    }

    // Store the sortable time stamp byte array.  This will be used for rk creation.
    val tsBytes = getTimeStampRowKeyBytes(span)
    val putsFuture = (baFutures ++ annoFutures).map { annoF =>
      annoF.map { case (anno, bytes) =>
        // Pulling out the parent here is safe because the parent must be set to find it here.
        val rk = Bytes.toBytes(anno.parent.get.id) ++ Bytes.toBytes(anno.id) ++ tsBytes
        val put = new Put(rk)
        put.add(TableLayouts.idxAnnotationFamily, Bytes.toBytes(span.traceId), bytes)
        put
      }
    }

    // Now put them into the table.
    Future.collect(putsFuture).flatMap { puts => idxServiceAnnotationTable.put(puts)  }
  }

  /**
   * Store the service name, so that we easily can
   * find out which services have been called from now and back to the ttl
   */
  private[hbase] def indexServiceName(span: Span): Future[Unit] = {
    val futureMappings = Future.collect(span.serviceNames.map { sn => serviceMapper.get(sn)}.toSeq)
    val timeBytes = getTimeStampRowKeyBytes(span)
    val putsFuture = futureMappings.map { mappings =>
      mappings.map { map =>
        val rk = Bytes.toBytes(map.id) ++ timeBytes
        val put = new Put(rk)
        put.add(TableLayouts.idxServiceFamily, Bytes.toBytes(span.traceId), Bytes.toBytes(true))
        put
      }
    }
    putsFuture.flatMap { puts => idxServiceTable.put(puts) }
  }

  /**
   * Index the span name on the service name. This is so we
   * can get a list of span names when given a service name.
   * Mainly for UI purposes
   */
  private[hbase] def indexSpanNameByService(span: Span): Future[Unit] = {
    val serviceMappingsFuture = span.serviceNames.map { sn => serviceMapper.get(sn)}.toSeq
    Future.collect(serviceMappingsFuture.map { smf =>
      smf.flatMap {_.spanNameMapper.get(span.name)}
    }).flatMap {
      snm => Future.Unit
    }
  }

  /**
   * Close the storage
   */
  override def close() {
    hbaseTable.close()
    idGenTable.close()
    mappingTable.close()

    //ttl tables.
    idxServiceTable.close()
    idxServiceAnnotationTable.close()
    idxServiceSpanNameTable.close()
  }

  /**
   * Get the available trace information from the storage system.
   * Spans in trace should be sorted by the first annotation timestamp
   * in that span. First event should be first in the spans list.
   */
  override def getSpansByTraceIds(traceIds: Seq[Long]): Future[Seq[Seq[Span]]] = {
    hbaseTable.get(createTraceGets(traceIds)).map { rl =>
      rl.map { result =>
        val spans = resultToSpans(Option(result)).sortBy { span => getTimeStamp(span)}
        spans
      }
    }
  }

  //
  // Internal Helper Methods.
  //

  private[this] def indexResultToTraceId(result: Result): Seq[IndexedTraceId] = {
    val rowLen = result.getRow.length
    val tsBytes = result.getRow.slice(rowLen - Bytes.SIZEOF_LONG, rowLen)
    val ts = Long.MaxValue - Bytes.toLong(tsBytes)
    result.list().asScala.map { kv =>
      IndexedTraceId(Bytes.toLong(kv.getQualifier), ts)
    }
  }

  private[this] def getTraceIdsByNameNoSpanName(serviceName: String, endTs: Long, limit: Int): Future[Seq[Result]] = {
    val serviceMappingFuture = serviceMapper.get(serviceName)
    serviceMappingFuture.flatMap { serviceMapping =>

      val scan = new Scan()
      // Ask for more rows because there can be large number of dupes.
      scan.setCaching(limit * 10)

      val startRk = Bytes.toBytes(serviceMapping.id) ++ getEndScanTimeStampRowKeyBytes(endTs)
      val endRk =  Bytes.toBytes(serviceMapping.id) ++ Bytes.toBytes(Long.MaxValue)
      scan.setStartRow(startRk)
      scan.setStopRow(endRk)
      // TODO(eclark): make this go back to the region server multiple times with a smart filter.
      idxServiceTable.scan(scan, limit*10)
    }
  }

  private[this] def getTraceIdsByNameWithSpanName(serviceName: String, spanName: String, endTs: Long, limit: Int): Future[Seq[Result]] = {
    val serviceMappingFuture = serviceMapper.get(serviceName)
    serviceMappingFuture.flatMap { serviceMapping =>
      val spanNameMappingFuture = serviceMapping.spanNameMapper.get(spanName)
      spanNameMappingFuture.flatMap { spanNameMapping =>
        val scan = new Scan()
        val startRow = Bytes.toBytes(serviceMapping.id) ++ Bytes.toBytes(spanNameMapping.id) ++ getEndScanTimeStampRowKeyBytes(endTs)
        val stopRow = Bytes.toBytes(serviceMapping.id) ++ Bytes.toBytes(spanNameMapping.id) ++ Bytes.toBytes(Long.MaxValue)
        scan.setStartRow(startRow)
        scan.setStopRow(stopRow)
        idxServiceSpanNameTable.scan(scan, limit)
      }
    }
  }

  /**
   * This creates an HBase Get request for a Seq of traces.
   * @param traceIds All of the traceId's that are requested.
   * @return Seq of Get Requests.
   */
  private[this] def createTraceGets(traceIds: Seq[Long]): Seq[Get] = {
    traceIds.map { id =>
      val g = new Get(Bytes.toBytes(id))
      g.setMaxVersions(1)
      g.addFamily(TableLayouts.storageFamily)
    }
  }

  private[this] def resultToSpans(option: Option[Result]): Seq[Span] = {
    val lists: Seq[KeyValue] = option match {
      case Some(result) => result.list().asScala
      case None => Seq.empty[KeyValue]
    }

    val spans: Seq[Span] = lists.map { kv =>
      serializer.fromBytes(kv.getValue).toSpan
    }
    spans
  }

  private[this] def rowKeyFromSpan(span: Span): Array[Byte] = Bytes.toBytes(span.traceId)
}
