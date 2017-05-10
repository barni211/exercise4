//package wdsr.exercise4.sender;
//
//import java.util.Enumeration;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.Semaphore;
//import java.util.concurrent.TimeUnit;
//
//import javax.jms.JMSException;
//import javax.jms.MapMessage;
//import javax.jms.Message;
//import javax.jms.ObjectMessage;
//import javax.jms.TextMessage;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.jms.annotation.JmsListener;
//
//public class UniversalJmsListener {
//	private static final Logger log = LoggerFactory.getLogger(UniversalJmsListener.class);
//
//	private volatile Semaphore semaphore = new Semaphore(0);
//
//	private String receivedText;
//	private Message receivedObjectMessage;
//	private Map<String, String> receivedMap;
//	private volatile boolean wasPubSub;
//
//	@JmsListener(destination = SendTest.QUEUE_NAME, containerFactory = "jmsQueueListenerContainerFactory")
//	public void processMessageFromQueue(Message msg) {
//		wasPubSub = false;
//		processMessage(msg);
//	}
//
//	@JmsListener(destination = SendTest.TOPIC_NAME, containerFactory = "jmsTopicListenerContainerFactory")
//	public void processMessageFromTopic(Message msg) {
//		wasPubSub = true;
//		processMessage(msg);
//	}
//
//	private void processMessage(Message msg) {
//		if (msg instanceof TextMessage) {
//			TextMessage textMsg = (TextMessage) msg;
//			synchronized (this) {
//				try {
//					receivedText = textMsg.getText();
//				} catch (JMSException e) {
//					log.error("Error when retrieving payload", e);
//				}
//			}
//		} else if (msg instanceof ObjectMessage) {
//			ObjectMessage objMsg = (ObjectMessage) msg;
//			synchronized (this) {
//				receivedObjectMessage = objMsg;
//			}
//		} else if (msg instanceof MapMessage) {
//			MapMessage mapMsg = (MapMessage) msg;
//			synchronized (this) {
//				try {
//					receivedMap = new HashMap();
//					Enumeration mapNames = mapMsg.getMapNames();
//					while (mapNames.hasMoreElements()) {
//						String key = (String) mapNames.nextElement();
//						receivedMap.put(key, mapMsg.getString(key));
//					}
//				} catch (JMSException e) {
//					log.error("Error when retrieving payload", e);
//				}
//			}
//		} else {
//			throw new IllegalArgumentException("Unsupported message type for message " + msg);
//		}
//		semaphore.release();
//
//	}
//
//	public synchronized String getReceivedText() {
//			return receivedText;
//	}
//
//	public synchronized Message getReceivedObjectMessage() {
//			return receivedObjectMessage;
//	}
//
//	public synchronized Map getReceivedMap() {
//			return receivedMap;
//	}
//
//	public boolean wasPubSub() {
//		return wasPubSub;
//	}
//
//	public void waitForMessage(long timeout, TimeUnit unit) throws InterruptedException {
//		semaphore.tryAcquire(timeout, unit);
//	}
//
//	public void reset() {
//		semaphore = new Semaphore(0);
//		synchronized (this) {
//			receivedText = null;
//			receivedObjectMessage = null;
//			receivedMap = null;
//			wasPubSub = false;
//		}
//	}
//}
