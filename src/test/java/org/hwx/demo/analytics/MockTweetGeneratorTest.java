package org.hwx.demo.analytics;

import java.io.InputStream;
import java.util.concurrent.LinkedBlockingQueue;

import org.hwx.demo.analytics.utils.IOUtils;
import org.hwx.demo.analytics.utils.SerializationUtils;

import twitter4j.Status;

/**
 * @author Nagaraj Jayakumar
 *
 */
public class MockTweetGeneratorTest {
	
	public static void main(String[] args) {
		LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);
		InputStream input = IOUtils.getInputStream("ser/tweets.ser");
		queue = SerializationUtils.deserialize(input);
	}
}
