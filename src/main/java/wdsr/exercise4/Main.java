package wdsr.exercise4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.sender.JmsSender;


public class Main {
	private static final Logger log = LoggerFactory.getLogger(Main.class);
	
	public static void main(String[] args) {
		log.info("\nEnter application.");
		
		JmsSender sender = new JmsSender("barni211.TOPIC");
		sender.sendTextToQueue();
		
		log.info("\nEnd application");
	}

}
