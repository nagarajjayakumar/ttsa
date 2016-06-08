package org.hwx.demo.analytics.bolts;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.hwx.demo.analytics.common.StemPolarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.twitter.common.text.token.attribute.TokenType;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author Nagaraj Jayakumar
 * Based on trained data set, each tweet will be classified
 */
public class TweetSentimentAnalyzerBolt extends BaseRichBolt {

	/**
	 * Generated Serial Version UID
	 */
	private static final long serialVersionUID = -3741720004045349349L;

	public static final String ID = "TweetSentimentAnalyzerBolt";
	public static final String CONF_LOGGING = ID + ".logging";

	private OutputCollector collector;

	Logger LOG = LoggerFactory.getLogger(TweetSentimentAnalyzerBolt.class);

	private boolean isLogEnabled = false;

	private SortedMap<String, Integer> stemPolarityMap = null;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		stemPolarityMap = Maps.newTreeMap();
		// Optional set logging
		if (stormConf.get(CONF_LOGGING) != null) {
			isLogEnabled = (Boolean) stormConf.get(CONF_LOGGING);
		} else {
			isLogEnabled = false;
		}
		try {
			final URL url = TweetSentimentAnalyzerBolt.class.getResource("/dictionaries/dictionary.tsv");
			final String text = Resources.toString(url, Charsets.UTF_8);
			final Iterable<String> lineSplit = Splitter.on("\n").trimResults().omitEmptyStrings().split(text);
			List<String> csvSplit;
			for (final String str : lineSplit) {
				csvSplit = Lists.newArrayList(Splitter.on("\t").trimResults().omitEmptyStrings().split(str));
				stemPolarityMap.put(csvSplit.get(2), StemPolarity.INSTANCE.geStemtPolarity(csvSplit.get(5)));
			}
		} catch (final IOException ioException) {
			if (isLogEnabled)
				LOG.error(ioException.getMessage(), ioException);
			ioException.printStackTrace();
			// Should not occur. If it occurs, we cant continue. So, exiting at
			// this point itself.
			System.exit(1);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		long tweetId = input.getLongByField("tweetid");
		String user = input.getStringByField("user");
		String userhandle = input.getStringByField("userhandle");
		String tweet = (String) input.getValueByField("tweet");
		Date _createdDate = (Date) input.getValueByField("time");
		
		SimpleDateFormat format = 
	        		new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	       
	    String createdDate = format.format(_createdDate);
		
	    String location = input.getStringByField("location");
		String country = input.getStringByField("country");

		String hashtags = input.getStringByField("hashtags");
		String source = input.getStringByField("source");
		String lang = input.getStringByField("lang");

		long retweetCount = input.getLongByField("retweetcount");
		long favoriteCount = input.getLongByField("favoritecount");

		String mentions = input.getStringByField("mentions");
		String longitude = input.getStringByField("longitude");
		String latitude = input.getStringByField("latitude");
		String fullText = input.getStringByField("fulltext");
		
		String timeZone = input.getStringByField("timezone");
		
		int totalCurrentTweetSentiment = 0;
		Map<String, String> mapTokenTypeStem = (Map<String, String>) input.getValueByField("tokentypeandstem");
		
		for(String lemma:mapTokenTypeStem.keySet()){
			
			if (isPolarityAvailableForStem(mapTokenTypeStem.get(lemma))) {
				totalCurrentTweetSentiment += getPolarityForStem(mapTokenTypeStem.get(lemma), lemma );
			}
		}
		
		int normalizeCurrentTweetSentiment = 0;
		String currentTweetSentiment =  "neutral";
		
		if (totalCurrentTweetSentiment  > 0){
			normalizeCurrentTweetSentiment =1;
			currentTweetSentiment = "positive";
		}else if (totalCurrentTweetSentiment  < 0){
			normalizeCurrentTweetSentiment =-1;
			currentTweetSentiment = "negative";
		}

		collector.emit(input,
				new Values(tweetId, user, userhandle, tweet, createdDate, location, country, hashtags, source, lang,
						retweetCount, favoriteCount, mentions, longitude, latitude, fullText,
						timeZone, totalCurrentTweetSentiment,
						normalizeCurrentTweetSentiment, currentTweetSentiment));

	}

	private int getPolarityForStem(String tokenType, String stem) {

		if (TokenType.TOKEN.name.equalsIgnoreCase(tokenType)) {
			if (stemPolarityMap.containsKey(stem)) {
				return stemPolarityMap.get(stem);
			}
		} else {
			return getPolarityOf(stem);
		}
		return 0;
	}

	private boolean isPolarityAvailableForStem(String tokenType) {

		if (TokenType.TOKEN.name.equalsIgnoreCase(tokenType) || TokenType.EMOTICON.name.equalsIgnoreCase(tokenType)) {
			return true;
		}
		return false;
	}
	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		/*
		 * tweet_id,t_user,userhandle,tweet,created_time timestamp,location,hashtags,
		 * source,lang,retweetcount,favoritecount,mentions, longitude, 
		 * latitude, fulltext, timezone,totalcurrenttweetsentiment,normalizecurrenttweetsentiment, currenttweetsentiment

		 */
		declarer.declare(new Fields("tweet_id", "t_user", "userhandle", "tweet", "created_time", "location", "country", "hashtags",
				"source", "lang", "retweetcount", "favoritecount", "mentions", "longitude", "latitude", "fulltext", "timezone",
				"totalcurrenttweetsentiment",
				"normalizecurrenttweetsentiment", "currenttweetsentiment"));

	}

	public static final int getPolarityOf(CharSequence emoticon) {

		Preconditions.checkNotNull(emoticon);
		Preconditions.checkArgument(emoticon.length() > 0);

		char lastChar = emoticon.charAt(emoticon.length() - 1);
		if (lastChar == '(' || lastChar == '<') {
			return StemPolarity.SAD.getPolarity();
		}

		return StemPolarity.HAPPY.getPolarity();
	}

}
