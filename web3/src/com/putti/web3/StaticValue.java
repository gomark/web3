package com.putti.web3;

import org.apache.hadoop.hbase.util.Bytes;

public class StaticValue {
	public String projectId = "putti-project2";
	public String instanceId = "putti-bigtable1";
	public byte[] tableName = Bytes.toBytes("tbl-stock");
}
