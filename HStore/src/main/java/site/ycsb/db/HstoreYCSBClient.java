/**
 * Copyright (c) 2015-2016 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import com.stumbleupon.async.TimeoutException;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.workloads.CoreWorkload;

import org.apache.hugegraph.pd.client.PDClient;
import org.apache.hugegraph.pd.client.PDConfig;
import org.apache.hugegraph.store.HgKvEntry;
import org.apache.hugegraph.store.HgKvIterator;
import org.apache.hugegraph.store.HgOwnerKey;
import org.apache.hugegraph.store.HgStoreClient;
import org.apache.hugegraph.store.HgStoreSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import org.apache.kudu.ColumnSchema;
//import org.apache.kudu.Schema;
//import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toBytes;
import static org.apache.hugegraph.store.client.util.HgStoreClientUtil.toStr;
import static site.ycsb.Client.DEFAULT_RECORD_COUNT;
import static site.ycsb.Client.RECORD_COUNT_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.INSERT_ORDER_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.INSERT_ORDER_PROPERTY_DEFAULT;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;
import static site.ycsb.workloads.CoreWorkload.ZERO_PADDING_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.ZERO_PADDING_PROPERTY_DEFAULT;
//import static org.apache.kudu.Type.STRING;
//import static org.apache.kudu.client.KuduPredicate.ComparisonOp.EQUAL;
//import static org.apache.kudu.client.KuduPredicate.ComparisonOp.GREATER_EQUAL;

/**
 * Hstore client for YCSB framework. Example to load: <blockquote>
 *
 * <pre>
 * <code>
 * $ ./bin/ycsb load hstore -P workloads/workloada -threads 5
 * </code>
 * </pre>
 *
 * </blockquote> Example to run:  <blockquote>
 *
 * <pre>
 * <code>
 * ./bin/ycsb run hstore -P workloads/workloada -p kudu_sync_ops=true -threads 5
 * </code>
 * </pre>
 *
 * </blockquote>
 */
public class HstoreYCSBClient extends site.ycsb.DB {
  private static final Logger LOG = LoggerFactory.getLogger(HstoreYCSBClient.class);
  private static final int DEFAULT_NUM_CLIENTS = 1;

  private static final String PD_ADDRESSES = "pd_addresses";
  private static final String DEFAULT_PD_ADDRESSES = "127.0.0.1:8686";

  public static final String VETEX_TABLE_NAME = "g+v";
  public static final String OUT_EDGE_TABLE_NAME = "g+oe";
  public static final String IN_EDGE_TABLE_NAME = "g+ie";




  private static List<HgStoreClient> clients = new ArrayList<>();
  private static int clientRoundRobin = 0;
  private static boolean tableSetup = false;
  private String tableName;
  private HgStoreClient storeClient;
  private PDClient pdClient;
  private HgStoreSession graph;
  private String partitionSchema;
  private String pd_addresses_list;
  private int zeropadding;
  private boolean orderedinserts;

  @Override
  public void init() throws DBException {
    Properties prop = getProperties();
    this.tableName = prop.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);//通过参数设置压测表名

    this.zeropadding = Integer.parseInt(prop.getProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_PROPERTY_DEFAULT));
    if (prop.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
      this.orderedinserts = false;
    } else {
      this.orderedinserts = true;
    }

    this.pd_addresses_list = prop.getProperty(PD_ADDRESSES,DEFAULT_PD_ADDRESSES);


    initClient();

    PDConfig pdConfig = PDConfig.of(this.pd_addresses_list)
                                .setEnableCache(true);
    this.pdClient = PDClient.create(pdConfig);

    this.storeClient = HgStoreClient.create(pdClient);// 创建 HStoreClient

    this.graph = storeClient.openSession("hugegraph/g");//并发使用哪个session or storeClient


  }

  /**
   * Initialize the 'clients' member with the configured number of
   * clients.
   */
  private void initClients() throws DBException {
//    synchronized (HstoreYCSBClient.class) {
//      if (!clients.isEmpty()) {
//        return;
//      }
//
//      Properties prop = getProperties();
//
//      String masterAddresses = prop.getProperty(MASTER_ADDRESSES_OPT,
//                                                "localhost:7051");
//      LOG.debug("Connecting to the masters at {}", masterAddresses);
//
//      int numClients = getIntFromProp(prop, NUM_CLIENTS_OPT, DEFAULT_NUM_CLIENTS);
//      for (int i = 0; i < numClients; i++) {
//        clients.add( HgStoreClient.create(pdClient));
//      }
//    }
  }

  private void initClient() throws DBException {
    initClients();
    //synchronized (clients) {
    //  client = clients.get(clientRoundRobin++ % clients.size());
    //}
    //setupTable();
    
    //TODO: moren chuangjian le
  }

  private void setupTable() throws DBException {
    //Properties prop = getProperties();
    //synchronized (HstoreYCSBClient.class) {
    //  if (tableSetup) {
    //    return;
    //  }
    //  int numTablets = getIntFromProp(prop, PRE_SPLIT_NUM_TABLETS_OPT, 4);
    //  if (numTablets > MAX_TABLETS) {
    //    throw new DBException("Specified number of tablets (" + numTablets
    //        + ") must be equal " + "or below " + MAX_TABLETS);
    //  }
    //  int numReplicas = getIntFromProp(prop, TABLE_NUM_REPLICAS, DEFAULT_NUM_REPLICAS);
    //  long recordCount = Long.parseLong(prop.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
    //  if (recordCount == 0) {
    //    recordCount = Integer.MAX_VALUE;
    //  }
    //  int blockSize = getIntFromProp(prop, BLOCK_SIZE_OPT, BLOCK_SIZE_DEFAULT);
    //  int fieldCount = getIntFromProp(prop, CoreWorkload.FIELD_COUNT_PROPERTY,
    //                                  Integer.parseInt(CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT));
    //  final String fieldprefix = prop.getProperty(CoreWorkload.FIELD_NAME_PREFIX,
    //                                              CoreWorkload.FIELD_NAME_PREFIX_DEFAULT);
    //
    //  List<ColumnSchema> columns = new ArrayList<ColumnSchema>(fieldCount + 1);
    //
    //  ColumnSchema keyColumn = new ColumnSchema.ColumnSchemaBuilder(KEY, STRING)
    //                                           .key(true)
    //                                           .desiredBlockSize(blockSize)
    //                                           .build();
    //  columns.add(keyColumn);
    //  COLUMN_NAMES.add(KEY);
    //  for (int i = 0; i < fieldCount; i++) {
    //    String name = fieldprefix + i;
    //    COLUMN_NAMES.add(name);
    //    columns.add(new ColumnSchema.ColumnSchemaBuilder(name, STRING)
    //                                .desiredBlockSize(blockSize)
    //                                .build());
    //  }
    //  schema = new Schema(columns);
    //
    //  CreateTableOptions builder = new CreateTableOptions();
    //
    //  if (partitionSchema.equals("hashPartition")) {
    //    builder.setRangePartitionColumns(new ArrayList<String>());
    //    List<String> hashPartitionColumns = new ArrayList<>();
    //    hashPartitionColumns.add(KEY);
    //    builder.addHashPartitions(hashPartitionColumns, numTablets);
    //  } else if (partitionSchema.equals("rangePartition")) {
    //    if (!orderedinserts) {
    //      // We need to use ordered keys to determine how to split range partitions.
    //      throw new DBException("Must specify `insertorder=ordered` if using rangePartition schema.");
    //    }
    //
    //    String maxKeyValue = String.valueOf(recordCount);
    //    if (zeropadding < maxKeyValue.length()) {
    //      throw new DBException(String.format("Invalid zeropadding value: %d, zeropadding needs to be larger "
    //          + "or equal to number of digits in the record number: %d.", zeropadding, maxKeyValue.length()));
    //    }
    //
    //    List<String> rangePartitionColumns = new ArrayList<>();
    //    rangePartitionColumns.add(KEY);
    //    builder.setRangePartitionColumns(rangePartitionColumns);
    //    // Add rangePartitions
    //    long lowerNum = 0;
    //    long upperNum = 0;
    //    int remainder = (int) recordCount % numTablets;
    //    for (int i = 0; i < numTablets; i++) {
    //      lowerNum = upperNum;
    //      upperNum = lowerNum + recordCount / numTablets;
    //      if (i < remainder) {
    //        ++upperNum;
    //      }
    //      PartialRow lower = schema.newPartialRow();
    //      lower.addString(KEY, CoreWorkload.buildKeyName(lowerNum, zeropadding, orderedinserts));
    //      PartialRow upper = schema.newPartialRow();
    //      upper.addString(KEY, CoreWorkload.buildKeyName(upperNum, zeropadding, orderedinserts));
    //      builder.addRangePartition(lower, upper);
    //    }
    //  } else {
    //    throw new DBException("Invalid partition_schema specified: " + partitionSchema
    //        + ", must specify `partition_schema=hashPartition` or `partition_schema=rangePartition`");
    //  }
    //  builder.setNumReplicas(numReplicas);
    //
    //  try {
    //    client.createTable(tableName, schema, builder);
    //  } catch (Exception e) {
    //    if (!e.getMessage().contains("already exists")) {
    //      throw new DBException("Couldn't create the table", e);
    //    }
    //  }
    //  tableSetup = true;
    //}
  }

  private static int getIntFromProp(Properties prop,
                                    String propName,
                                    int defaultValue) throws DBException {
    String intStr = prop.getProperty(propName);
    if (intStr == null) {
      return defaultValue;
    } else {
      try {
        return Integer.valueOf(intStr);
      } catch (NumberFormatException ex) {
        throw new DBException("Provided number for " + propName + " isn't a valid integer");
      }
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      //this.pdClient;
      //this.graph.close();
    } catch (Exception e) {
      throw new DBException("Couldn't cleanup the session", e);
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    Vector<HashMap<String, ByteIterator>> results = new Vector<>();
    final Status status = scan(table, key, 1, fields, results);
    if (!status.equals(Status.OK)) {
      return status;
    }
    if (results.size() != 1) {
      return Status.NOT_FOUND;
    }
    result.putAll(results.firstElement());
    return Status.OK;
  }

  @Override
  public Status scan(String table,
                     String startkey,
                     int recordcount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {

    HgOwnerKey startkey1 = HgOwnerKey.of(toBytes(startkey), toBytes(startkey));

    HgKvIterator<HgKvEntry> iterator = graph.scanIterator(table,startkey1);

    int num = 0;
    while (iterator.hasNext()) {
      num ++;
      HgKvEntry entry = iterator.next();
      byte[] keyFromHStore = entry.key();
      byte[] valueFromHStore = entry.value();

      if(num >= recordcount){
        break;
      }
    }
    return Status.OK;
  }

  //private void addAllRowsToResult(RowResultIterator it,
  //                                int recordcount,
  //                                List<String> querySchema,
  //                                Vector<HashMap<String, ByteIterator>> result) throws Exception {
  //  RowResult row;
  //  HashMap<String, ByteIterator> rowResult = new HashMap<>(querySchema.size());
  //  if (it == null) {
  //    return;
  //  }
  //  while (it.hasNext()) {
  //    if (result.size() == recordcount) {
  //      return;
  //    }
  //    row = it.next();
  //    int colIdx = 0;
  //    for (String col : querySchema) {
  //      rowResult.put(col, new StringByteIterator(row.getString(colIdx)));
  //      colIdx++;
  //    }
  //    result.add(rowResult);
  //  }
  //}

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    HgOwnerKey key1 = HgOwnerKey.of(toBytes(key), toBytes(key));
    graph.put(table, key1, values.toString().getBytes());

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    HgOwnerKey key1 = HgOwnerKey.of(toBytes(key), toBytes(key));
    graph.put(table, key1, values.toString().getBytes());

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {

    HgOwnerKey key1 = HgOwnerKey.of(toBytes(key), toBytes(key));
    graph.delete(table, key1);
    graph.delete(table, key1);

    return Status.OK;
  }

  //private void apply(Operation op) {
  //  //try {
  //  //  OperationResponse response = session.apply(op);
  //  //  if (response != null && response.hasRowError()) {
  //  //    LOG.info("Write operation failed: {}", response.getRowError());
  //  //  }
  //  //} catch (KuduException ex) {
  //  //  LOG.warn("Write operation failed", ex);
  //  //}
  //}
}
