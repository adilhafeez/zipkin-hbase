package com.twitter.zipkin.storage.hbase

import com.twitter.util.Await
import com.twitter.zipkin.Constants
import com.twitter.zipkin.common.{Annotation, Endpoint, Span}
import com.twitter.zipkin.hbase.TableLayouts
import com.twitter.zipkin.storage.hbase.mapping.ServiceMapper
import com.twitter.zipkin.storage.hbase.utils.{IDGenerator, HBaseTable}
import org.apache.hadoop.hbase.client.{Scan, Get, HTable}
import org.apache.hadoop.hbase.util.Bytes

/**
 * This isn't really a great unit test but it's a good starting
 * point until I have a mock HBaseTable.
 */
class HBaseSpanStoreSpec extends ZipkinHBaseSpecification {

  val tablesNeeded = TableLayouts.tables.keys.toSeq

  val traceId = 100L
  val spanId = 567L
  val span = Span(traceId, "span.methodCall()", spanId, None, List(), List())

  val spanStore = new HBaseSpanStore(_conf)

  after {
    spanStore.close()
  }

  val endOfTime = Long.MaxValue
  def before(ts: Long) = ts - 1
  val traceIdOne = 100
  val spanOneStart = 90000L
  val serviceNameOne = "HBase.Client"
  val endpointOne = new Endpoint(0, 0, serviceNameOne)
  val annoOneList = List(
    new Annotation(spanOneStart, Constants.ClientSend, Some(endpointOne)),
    new Annotation(spanOneStart + 100, Constants.ClientRecv, Some(endpointOne))
  )
  val spanOneId: Long = 32003
  val spanOneName = "startingSpan"
  val spanOne = Span(traceIdOne, spanOneName, spanOneId, None, annoOneList, Seq())

  val spanTwoStart = spanOneStart + 100
  val serviceNameTwo = "HBase.RegionServer"
  val endPointTwo = new Endpoint(0, 0, serviceNameTwo)
  val annoTwoList = List(new Annotation(spanTwoStart, Constants.ServerRecv, Some(endPointTwo)))
  val spanTwo = Span(traceIdOne, "secondSpan", 45006, Some(spanOneId), annoTwoList, Seq())

  val spanThreeStart = spanTwoStart + 100
  val annoThreeList = List(new Annotation(spanThreeStart, Constants.ServerRecv, Some(endPointTwo)))
  val spanThree = Span(traceIdOne, "spanThree", 45007, Some(spanOneId), annoThreeList, Seq())

  val traceIdFour = 103
  val spanFourStart = spanThreeStart + 100
  val annoFourList = List(new Annotation(spanFourStart, Constants.ServerRecv, Some(endPointTwo)))
  val spanFour = Span(traceIdFour, "spanThree", 45008, None, annoFourList, Seq())


  val spanFiveStart = spanFourStart + 100
  val annoFiveValue = "CustomANNO"
  val annoFiveList = List(new Annotation(spanFiveStart, annoFiveValue, Some(endPointTwo)))
  val spanFive = Span(traceIdFour, "spanThree", 45009, Some(45006), annoFiveList, Seq())

  test("indexServiceName") {
    val serviceTable = new HBaseTable(_conf, TableLayouts.idxServiceTableName)

    val mappingTable = new HBaseTable(_conf, TableLayouts.mappingTableName)
    val idGenTable = new HBaseTable(_conf, TableLayouts.idGenTableName)
    val idGen = new IDGenerator(idGenTable)
    val serviceMapper = new ServiceMapper(mappingTable, idGen)

    Await.result(spanStore.indexServiceName(spanOne))
    val results = Await.result(serviceTable.scan(new Scan(), 100))
    results.size should be (1)

    val result = results.head
    result.getRow.size should be (Bytes.SIZEOF_LONG * 2)

    val serviceNameFromSpan = spanOne.serviceName.get
    val serviceMapping = Await.result(serviceMapper.get(serviceNameFromSpan))
    Bytes.toLong(result.getRow) should be (serviceMapping.id)
    Bytes.toLong(result.getRow.slice(Bytes.SIZEOF_LONG, Bytes.SIZEOF_LONG * 2)) should be (Long.MaxValue - spanOneStart)
  }

  test("indexTraceIdByServiceAndName") {
    val serviceSpanNameTable = new HBaseTable(_conf, TableLayouts.idxServiceSpanNameTableName)
    Await.result(spanStore.indexTraceIdByServiceAndName(spanOne))
    val scan = new Scan()
    val results = Await.result(serviceSpanNameTable.scan(scan, 100))
    results.size should be (1)
  }

  test("indexSpanByAnnotations") {
    val annoTable = new HBaseTable(_conf, TableLayouts.idxServiceAnnotationTableName)
    Await.result(spanStore.indexSpanByAnnotations(spanFive))
    val result = Await.result(annoTable.scan(new Scan(), 1000))
    result.size should be (1)
  }

  test("indexDuration") {
    val durationTable = new HBaseTable(_conf, TableLayouts.durationTableName)
    Await.result(spanStore.indexSpanDuration(spanOne))
    val result = Await.result(durationTable.scan(new Scan(), 1000))
    result.size should be (1)
  }

  test("getTracesDuration") {
    Await.result(spanStore.indexSpanDuration(spanOne))
    val durations = Await.result(spanStore.getTracesDuration(Seq(traceIdOne)))
    durations should not be (Seq())
    durations.map {_.duration} should contain(100)

    durations.map {_.traceId} should contain(traceIdOne)
  }

  test("getTraceIdsByName") {
    Await.result(spanStore.indexServiceName(spanOne))
    Await.result(spanStore.indexServiceName(spanTwo))
    Await.result(spanStore.indexServiceName(spanThree))
    Await.result(spanStore.indexServiceName(spanFour))

    Await.result(spanStore.indexTraceIdByServiceAndName(spanOne))
    Await.result(spanStore.indexTraceIdByServiceAndName(spanTwo))
    Await.result(spanStore.indexTraceIdByServiceAndName(spanThree))
    Await.result(spanStore.indexTraceIdByServiceAndName(spanFour))

    val emptyResult = Await.result(spanStore.getTraceIdsByName(serviceNameOne, None, before(spanOneStart), 1))
    emptyResult should be (Seq())

    // Try and get the first trace from the first service name
    val t1 = Await.result(spanStore.getTraceIdsByName(serviceNameOne, None, before(endOfTime), 1))
    t1.map {_.traceId} should contain(traceIdOne)
    t1.map {_.timestamp} should contain(spanOneStart)
    t1.size should be (1)

    // Try and get the first two traces from the second service name
    val t2 = Await.result(spanStore.getTraceIdsByName(serviceNameTwo, None, before(endOfTime), 100))
    t2.map {_.traceId} should contain(traceIdOne)
    t2.map {_.traceId} should contain(traceIdFour)
    t2.map {_.timestamp} should contain(spanTwoStart)
    t2.map {_.timestamp} should contain(spanThreeStart)

    // Try and get the first trace from the first service name and the first span name
    val t3 = Await.result(spanStore.getTraceIdsByName(serviceNameOne, Some(spanOne.name), before(endOfTime), 1))
    t3.map {_.traceId} should contain(traceIdOne)
    t3.map {_.timestamp} should contain(spanOneStart)
    t3.size should be (1)
  }

  test("getTraceIdsByAnnotation") {
    Await.result(spanStore.indexSpanByAnnotations(spanFive))
    val idf = spanStore.getTraceIdsByAnnotation(spanFive.annotations.head.serviceName, spanFive.annotations.head.value, None, before(endOfTime), 100)
    val ids = Await.result(idf)
    ids.size should be (1)
    ids.map {_.traceId} should contain(spanFive.traceId)
  }

  test("storeSpan") {
    Await.result(spanStore.apply(Seq(span)))
    // The data should be there by now.
    val htable = new HTable(_conf, TableLayouts.storageTableName)
    val result = htable.get(new Get(Bytes.toBytes(traceId)))
    result.size shouldEqual 1
  }

  test("tracesExist") {
    // Put the span just in case the ordering changes.
    Await.result(spanStore.apply(Seq(span)))
    val idsFound = Await.result(spanStore.tracesExist(Seq(traceId, 3002L)))
    idsFound should contain(traceId)
    idsFound.size should be (1)
  }

  test("getSpansByTraceId") {
    Await.result(spanStore.apply(Seq(span)))
    val spansFound = spanStore.getSpansByTraceId(traceId)
    Await.result(spansFound) should contain(span)
  }

  test("getSpansByTraceIds") {
    Await.result(spanStore.apply(Seq(span)))
    val spansFoundFuture = spanStore.getSpansByTraceIds(Seq(traceId, 302L))
    val spansFound = Await.result(spansFoundFuture).flatten
    spansFound should contain(span)
    spansFound.size should be (1)
  }
}
