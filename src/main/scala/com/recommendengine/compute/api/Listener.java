/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.recommendengine.compute.api;

import java.util.Map;
import java.util.Map.Entry;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.QueueImpl;
import org.apache.qpid.amqp_1_0.jms.impl.TopicImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.recommendengine.compute.conf.ComputingConfiguration;
import com.recommendengine.compute.db.hbase.FileMappingToHbase;
import com.recommendengine.compute.db.hbase.HbaseServer;
import com.recommendengine.compute.metadata.Computing;
import com.recommendengine.compute.utils.TableUtil;

public class Listener {

	public static Logger log = LoggerFactory.getLogger(Listener.class);
	public static long start = System.currentTimeMillis();
	public static long count = 1;
	public static Gson gson = new Gson();

	private static Configuration conf = HBaseConfiguration.create();

	public static void main(String[] args) throws JMSException {

		// System.out.println("2016050318000010".hashCode());
		// System.out.println(Integer.MAX_VALUE);
		Configuration conf = ComputingConfiguration.createWithHbase();
		conf.set(Computing.COMPUTING_ID(), "test22");

		start(args, conf);

	}

	private static String env(String key, String defaultValue) {
		String rc = System.getenv(key);
		if (rc == null)
			return defaultValue;
		return rc;
	}

	private static String arg(String[] args, int index, String defaultValue) {
		if (index < args.length)
			return args[index];
		else
			return defaultValue;
	}

	private static void beforeStart(Configuration conf) {

		String bizCode = conf.get(Computing.COMPUTING_ID());
		if (bizCode == null)
			throw new IllegalArgumentException(" 业务代码不能为空");
		String tableName = conf.get("preferred.table.name");

		Map<String, HTableDescriptor> tables = FileMappingToHbase
				.readMappingFile(conf.get("hbase.default.mapping.file"));

		for (HTableDescriptor desc : tables.values()) {
			if (tableName == null)

				FileMappingToHbase.createTable(desc, conf, bizCode);
		}

	}

	public static void start(String[] args, Configuration conf)
			throws JMSException {

		if (conf == null)
			conf = ComputingConfiguration.create();

		String user = conf.get("imrec.activeMQ.user", "admin");
		String password = conf.get("imrec.activeMQ.password", "password");
		String host = conf.get("imrec.activeMQ.host", "work.net");
		int port = conf.getInt("imrec.activeMQ.port", 5673);
		log.info("start activemq,user=" + user + ",host=" + host + ":" + port);
		// beforeStart(conf);
		// String user = env("ACTIVEMQ_USER", "admin");
		// String password = env("ACTIVEMQ_PASSWORD", "password");
		// String host = env("ACTIVEMQ_HOST", "work.net");
		// int port = Integer.parseInt(env("ACTIVEMQ_PORT", "5673"));

		String destination = arg(args, 0, "topic://emall");
		ConnectionFactoryImpl factory = new ConnectionFactoryImpl(host, port,user, password);
		Destination dest = null;
		if (destination.startsWith("topic://")) {
			dest = new TopicImpl(destination);
		} else {
			dest = new QueueImpl(destination);
		}

		Connection connection = factory.createConnection();
		connection.setClientID("topClient1");
		connection.start();
		final Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);
		// MessageConsumer consumer = session.createConsumer(dest);
		// Queue queue = session.createQueue("event");
		// MessageConsumer consumer = session.createConsumer(queue);
		// Topic topic = session.createTopic("topic://topicName");

		final MessageConsumer consumer = session.createDurableSubscriber(
				new TopicImpl(destination), Listener.class.getName());
		System.out.println("Waiting for messages...");
		consumer.setMessageListener(new MessageListener() {
			public void onMessage(Message msg) {
				if (msg instanceof TextMessage) {
					TextMessage textMsg = (TextMessage) msg;

					try {
						String body = textMsg.getText(); // {"createTime":"1463103981104","goodsId":"2016050318000010",
															// "userId":"00002667","type":"attention"}

						
						if ("SHUTDOWN".equals(body)) {

							// System.out.println(body);
							// consumer.close();
							// session.unsubscribe("sub1");
						} else {
							System.out.println(String.format(
									"Received %d messages.", count));
						}
					} catch (JMSException e) {
						e.printStackTrace();
					}
					count++;
				} else {
					System.out.println("Unexpected message type: "
							+ msg.getClass());
				}
			}
		});

	}

	private static Put toSave(Map<String, Object> args) {

		String type = (String) args.get("type");
		String userId = (String) args.get("userId");
		Put put = new Put(userId.getBytes());

		String goodId = (String) args.get("goodsId");
		Long time = Long.valueOf(String.valueOf(args.get("createTime")));
		if (type.equals("AttentionGoods")) {

			put.add(type.getBytes(), goodId.getBytes(), Bytes.toBytes(time));

		} else if (type.equals("BrowseGoods")) {

			put.add(type.getBytes(), goodId.getBytes(), Bytes.toBytes(time));

		} else if (type.equals("BuyGoods")) {

			put.add(type.getBytes(), goodId.getBytes(), Bytes.toBytes(time));
		}

		return null;

	}
}

class Recored {

	private long lastTime;
	private String userId;
	private String goodsId;
	private String type;
	private float value = Float.NaN;

	public Recored() {

	}

	public Recored(String userId, String goodsId, String type, float value,
			long lastTime) {
		super();
		this.lastTime = lastTime;
		this.userId = userId;
		this.goodsId = goodsId;
		this.type = type;
		this.value = value;
	}

	public long getLastTime() {
		return lastTime;
	}

	public void setLastTime(long lastTime) {
		this.lastTime = lastTime;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getGoodsId() {
		return goodsId;
	}

	public void setGoodsId(String goodsId) {
		this.goodsId = goodsId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public float getValue() {
		return value;
	}

	public void setValue(float value) {
		this.value = value;
	}

	public Put toPut() {
		if (userId == null || goodsId == null)
			return null;
		float pref = 1;
		if (this.value != Float.NaN)
			pref = value;

		Put put = new Put(userId.getBytes());
		put.add(type.getBytes(), goodsId.getBytes(), Bytes.toBytes(pref));
		// put.add
		return put;
	}

}
