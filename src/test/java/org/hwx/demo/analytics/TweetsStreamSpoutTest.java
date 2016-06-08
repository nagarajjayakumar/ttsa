package org.hwx.demo.analytics;

import java.util.concurrent.LinkedBlockingQueue;

import org.hwx.demo.analytics.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * @author Nagaraj Jayakumar
 * Test class for the twitter stream
 */
@SuppressWarnings("serial")
public class TweetsStreamSpoutTest {

	private static final Logger LOG = LoggerFactory.getLogger(TweetsStreamSpoutTest.class);
	 ;

	

	public static void main(String[] args) throws TwitterException {
		final LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
		String[] keyWords = {}; 
		String filterLanguage = "en";
		StatusListener listener = new StatusListener() {
			long maxQueueDepth = 1000;

			@Override
			public void onStatus(Status status) {
				if (queue.size() < maxQueueDepth) {
					queue.offer(status);
					System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
				} else {
					SerializationUtils.serializeCollection(queue, "tweets.ser");
					LOG.error("Done Writing Serial File " + queue.size());
					System.exit(1);
				}
			}

			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			public void onTrackLimitationNotice(int i) {
			}

			public void onScrubGeo(long l, long l1) {
			}

			public void onException(Exception ex) {
			}

			public void onStallWarning(StallWarning arg0) {

			}

		};

		twitterStream = new TwitterStreamFactory()
				.getInstance();

		twitterStream.addListener(listener);
		
		if (keyWords.length == 0) {
				twitterStream.sample();
		 }

	}

}
