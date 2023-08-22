/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rapimoney;


import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;


public class DataStreamJob {


	public static void main( String[] args ) throws Exception{

//		SourceFunction<String> sourceFunction = getPostgresSource();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//		env.addSource(sourceFunction)
//				.print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

		// create a user stream
		DataStream<Row> userStream = env
				.fromElements(
						Row.of(LocalDateTime.parse("2021-08-21T13:00:00"), 1, "Alice"),
						Row.of(LocalDateTime.parse("2021-08-21T13:05:00"), 2, "Bob"),
						Row.of(LocalDateTime.parse("2021-08-21T13:10:00"), 2, "Bob"))
				.returns(
						Types.ROW_NAMED(
								new String[] {"ts", "uid", "name"},
								Types.LOCAL_DATE_TIME, Types.INT, Types.STRING));

		tableEnv.createTemporaryView(
				"UserTable",
				userStream,
				Schema.newBuilder()
						.column("ts", DataTypes.TIMESTAMP(3))
						.column("uid", DataTypes.INT())
						.column("name", DataTypes.STRING())
						.watermark("ts", "ts - INTERVAL '1' SECOND")
						.build());


		Table selectTable = tableEnv.sqlQuery("SELECT name, uid FROM UserTable");
		DataStream<Row> joinedStream = tableEnv.toDataStream(selectTable);
		joinedStream.print();


		Table resultTable = tableEnv.sqlQuery("SELECT name, SUM(uid) FROM UserTable GROUP BY name");
		DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);
		resultStream.print();

		env.execute();
	}
	public static SourceFunction<String> getPostgresSource() throws Exception {
		SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
				.hostname("localhost")
				.port(5432)
				.database("mydatabase") // monitor postgres database
				.schemaList("public")  // monitor inventory schema
				.tableList("public.shipments") // monitor products table
				.username("myuser")
				.password("secret")
				.decodingPluginName("pgoutput")
				.slotName("flinktest1")
				.deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
				.build();

		return sourceFunction;


	}
}
