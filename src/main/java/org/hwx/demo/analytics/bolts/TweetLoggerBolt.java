package org.hwx.demo.analytics.bolts;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author Nagaraj Jayakumar
 * Logger bolt - we can enable or disable based on configuration
 */
public class TweetLoggerBolt extends BaseRichBolt {

	/**
	*  Generated Serial Version UID
	*/
	private static final long serialVersionUID = -6145293241324093335L;
	Logger LOG = LoggerFactory.getLogger(TweetLoggerBolt.class); 
	
	long count = 0 ;
	@SuppressWarnings("unused")
	private OutputCollector collector;
    
    
    public static final String ID = "TweetLoggerBolt";
	public static final String CONF_LOGGING = ID + ".logging";
	private boolean isLogEnabled = false;
	
	@Override
	public void execute(Tuple input) {
		
		
		long tweetId = input.getLongByField("tweet_id");
		String country = (String)input.getValueByField("country");
		int totalCurrentTweetSentiment = input.getIntegerByField("totalcurrenttweetsentiment");
		String currentTweetSentiment =  input.getStringByField("currenttweetsentiment");
		String tweet = (String) input.getValueByField("tweet");
		String created_time =(String) input.getValueByField("created_time");
		if(isLogEnabled)
			LOG.info(++count +" "+created_time+" " +country+" " +tweetId +" " + currentTweetSentiment + " " + totalCurrentTweetSentiment + " " + tweet);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
        if (stormConf.get(CONF_LOGGING) != null) {
			isLogEnabled = (Boolean) stormConf.get(CONF_LOGGING);
		} else {
			isLogEnabled = false;
		}
		
	}

	

	
}
