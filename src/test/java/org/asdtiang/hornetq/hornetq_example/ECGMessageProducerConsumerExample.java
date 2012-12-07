package org.asdtiang.hornetq.hornetq_example;

import java.util.Date;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.naming.NamingException;

import org.junit.Test;

public class ECGMessageProducerConsumerExample {

	javax.naming.Context ic = null;
	javax.jms.ConnectionFactory cf = null;
	javax.jms.Connection connection = null;
	javax.jms.Queue queue = null;
	javax.jms.Session session = null;
	com.mongodb.Mongo m;
	com.mongodb.DB db;

	@Test
	public void test() throws NamingException, JMSException {

		String destinationName = "queue/DLQ";
		java.util.Properties p = new java.util.Properties();
		p.put(javax.naming.Context.INITIAL_CONTEXT_FACTORY,
				"org.jnp.interfaces.NamingContextFactory");
		p.put(javax.naming.Context.URL_PKG_PREFIXES,
				"org.jboss.naming:org.jnp.interfaces");
		p.put(javax.naming.Context.PROVIDER_URL, "jnp://localhost:1099");

		ic = new javax.naming.InitialContext(p);

		cf = (javax.jms.ConnectionFactory) ic.lookup("/ConnectionFactory");
		queue = (javax.jms.Queue) ic.lookup(destinationName);
		connection = cf.createConnection();
		/**
		 * static int AUTO_ACKNOWLEDGE 该确认模式下，在会话成功地从 receive
		 * 调用返回或会话为处理消息而调用的消息监听器成功的返回时，该会话自动确认客户端消息的接收。 static int
		 * CLIENT_ACKNOWLEDGE 该确认模式下，客户端通过调用消息的 acknowledge 方法来确认被消费的消息。 static
		 * int DUPS_OK_ACKNOWLEDGE 该确认模式通知会话延迟确认消息的传递。 static int
		 * SESSION_TRANSACTED 如果会话是事务性的，该值从 getAcknowledgeMode 方法返回。
		 */
		session = connection.createSession(false,
				javax.jms.Session.AUTO_ACKNOWLEDGE);
		connection.start();

		String theECG = "1;"+ (new Date()).getTime()+";1020,1021,1022";
		javax.jms.MessageProducer publisher = session.createProducer(queue);
		javax.jms.TextMessage message = session.createTextMessage(theECG);
		/**
		 * public void send(Message message, int deliveryMode, int priority,
		 * long timeToLive) throws JMSException使用指定的传送模式，优先级和存和时间发送消息到指定的目的地。
		 * Parameters: message - 待发送的消息 deliveryMode - 传送模式 :deliveryMode -
		 * 该消息生产者的消息传送模式；合法的值有 DeliveryMode.NON_PERSISTENT 和
		 * DeliveryMode.PERSISTENT 默认为持久化 priority - 消息优先级JMS
		 * API定义了10种级别的优先级值，0为最低优先级
		 * ，9为最高优先级。客户端应当认为0-4为常规优先级，5-9为加急优先级。优先级缺省设置为4。 timeToLive -
		 * 消息的生存周期（以毫秒为单位
		 * ）设置自发送消息时刻起消息系统应当保留生产的消息的默认时长，缺省生存时间为0。以毫秒为单位的消息生存时间；0表示不超时
		 */
		publisher.send(message);
		System.out.println("Message sent!");
		publisher.close();
		javax.jms.MessageConsumer messageConsumer = session
				.createConsumer(queue);
		javax.jms.TextMessage messageReceived = (TextMessage) messageConsumer
				.receive(5000);
		insertMongo(messageReceived);
		System.out.println("Received message: " + messageReceived.getText());
		messageConsumer.close();

		if (session != null) {
			try {
				session.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}
		if (connection != null) {
			try {
				connection.close();
			} catch (JMSException e) {
				e.printStackTrace();
			}
		}

	}

	private void insertMongo(TextMessage textMessage) {

		try {
			m = new com.mongodb.Mongo();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		db = m.getDB("hornetqdb");
		com.mongodb.DBCollection coll = db.getCollection("testCollection");
		com.mongodb.BasicDBObject doc = new com.mongodb.BasicDBObject();
		doc.put("name", "MongoDB");
		doc.put("type", "database");
		try {
			doc.put("textmessage", textMessage.getText());
			int result = coll.insert(doc).getN();
			System.out.println("write result:" + result);
			System.out.println("insert ok:" +textMessage.getText() );
	        if (m != null)
	            m.close();
	        m = null;
	        db = null;
	        System.gc();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
