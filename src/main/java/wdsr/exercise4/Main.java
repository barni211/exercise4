package wdsr.exercise4;

import java.util.ArrayList;

import javax.jms.JMSException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.receiver.JmsQueueReceiver;

public class Main {
	private static final Logger log = LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) throws JMSException {
		// TODO Auto-generated method stub
		log.info("Enter application.");
		
		JmsQueueReceiver receiver = new JmsQueueReceiver("BARNI211.QUEUE");
		receiver.getMessages();
		
		log.info("Received " + receiver.getSize() + " messages.");
		log.info("Exit application.");

	}

}
