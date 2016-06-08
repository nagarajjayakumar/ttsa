package org.hwx.demo.analytics.spouts;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * @author Nagaraj Jayakumar
 * Spout connects to twitter, receives tweets and emit the tweet tuple.
 */

public class TweetsStreamSpout extends BaseRichSpout {

	/**
	 * Generated Serial Version ID
	 */
	private static final long serialVersionUID = 4153142990654101563L;
	SpoutOutputCollector collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream twitterStream;
	long maxQueueDepth;

	Logger LOG = LoggerFactory.getLogger(TweetsStreamSpout.class);

	public static final String ID = "TweetsStreamSpout";
	public static final String CONF_LOGGING = ID + ".logging";
	private boolean isLogEnabled = false;
	private String  filterLanguage;
	
	
	String[] keyWords = {};
	private boolean applyLocationFilter=false;

	public TweetsStreamSpout() {
	}

	public TweetsStreamSpout(String[] trackTerms, long maxQueueDepth, String filterLanguage, boolean applyLocationFilter) {
		this.keyWords = trackTerms;
		this.maxQueueDepth = maxQueueDepth;
		this.filterLanguage  = filterLanguage;
		this.applyLocationFilter = applyLocationFilter;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		if (conf.get(CONF_LOGGING) != null) {
			isLogEnabled = (Boolean) conf.get(CONF_LOGGING);
		} else {
			isLogEnabled = false;
		}
		
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				if (queue.size() < maxQueueDepth) {
					if(isLogEnabled)
						LOG.trace("TWEET Received: " + status);
					queue.offer(status);
				} else {
					if(isLogEnabled)
						LOG.error("Queue is now full, the following message is dropped: " + status);
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

		twitterStream = new TwitterStreamFactory(new ConfigurationBuilder().setJSONStoreEnabled(true).build())
				.getInstance();

		twitterStream.addListener(listener);
		
		// Filter twitter stream
	    FilterQuery tweetFilterQuery = new FilterQuery();
	    if (keyWords != null && keyWords.length > 0 ) {
	      tweetFilterQuery.track(keyWords);
	    }

	    // Filter location
	    //Bounding Box for United States.
		
	    if(applyLocationFilter){
			final double[][] boundingBoxOfUS = {{-124.848974, 24.396308},
				                                   {-66.885444, 49.384358}};
			tweetFilterQuery.locations(boundingBoxOfUS);// any geotagged tweet
		}

	    // Filter language
	    tweetFilterQuery.language(new String[] { filterLanguage });

	    twitterStream.filter(tweetFilterQuery);

	}

	public void nextTuple() {
		Status status = queue.poll();
		if (status == null) {
			Utils.sleep(50);
		} else {
			/*
			 * Disclaimer: Beans will work fine for shuffle grouping. 
			 * If you need to do a fieldsGrouping, you should still use a primitive. 
			 */
			collector.emit(new Values(status));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

}
