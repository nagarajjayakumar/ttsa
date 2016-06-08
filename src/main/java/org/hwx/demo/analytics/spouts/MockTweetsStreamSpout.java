package org.hwx.demo.analytics.spouts;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.hwx.demo.analytics.utils.IOUtils;
import org.hwx.demo.analytics.utils.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.Status;

/**
 * @author Nagaraj Jayakumar
 * Mock twitter stream - reads tweets from File
 */
public class MockTweetsStreamSpout extends BaseRichSpout {

	/**
	 * Generated Serial Version UID
	 */
	private static final long serialVersionUID = -6355221939226055115L;
	SpoutOutputCollector collector;
	LinkedBlockingQueue<Status> queue = null;
	long maxQueueDepth;

	Logger LOG = LoggerFactory.getLogger(MockTweetsStreamSpout.class);

	public static final String ID = "MockTweetsStreamSpout";
	public static final String CONF_LOGGING = ID + ".logging";

	String[] keyWords = {};

	public MockTweetsStreamSpout() {
	}

	public MockTweetsStreamSpout(String[] trackTerms, long maxQueueDepth, String filterLanguage) {
		this.keyWords = trackTerms;
		this.maxQueueDepth = maxQueueDepth;
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		InputStream input = IOUtils.getInputStream("ser/tweets.ser");
		queue = SerializationUtils.deserialize(input);

	}

	public void nextTuple() {
		Status status = queue.poll();
		if (status == null) {
			Utils.sleep(50);
		} else {
			/*
			 * Disclaimer: Beans will work fine for shuffle grouping. If you
			 * need to do a fieldsGrouping, you should still use a primitive.
			 */
			collector.emit(new Values(status));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

}
