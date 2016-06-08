package org.hwx.demo.analytics.bolts;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.twitter.common.text.DefaultTextTokenizer;
import com.twitter.common.text.token.TokenizedCharSequence;
import com.twitter.common.text.token.TokenizedCharSequence.Token;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.washington.cs.knowitall.morpha.MorphaStemmer;

/**
 * @author Nagaraj Jayakumar
 * the words in a tweet are broken down into tokens
 * Emoticons, abbreviations, hashtags and URLs are recognized
   as individual tokens
 */
public class TweetTokenizerBolt extends BaseRichBolt {

    /**
	 * Generated Serial Version UID
	 */
	private static final long serialVersionUID = 8909887029987194074L;

	public static final String ID = "TweetTokenizerBolt";
	public static final String CONF_LOGGING = ID + ".logging";
	
	private OutputCollector collector;
    
    Logger LOG = LoggerFactory.getLogger(TweetTokenizerBolt.class); 
    
    private boolean isLogEnabled = false;
    
    private final static List<String> stopWords = Arrays.asList(
		      "a", "an", "and", "are", "as", "at", "be", "but", "by",
		      "for", "if", "in", "into", "is", "it",
		      "no", "not", "of", "on", "or", "such",
		      "that", "the", "their", "then", "there", "these",
		      "they", "this", "to", "was", "will", "with"
		    );
    
    private SortedMap<String, String> timeZoneCountryMap = null;
    
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        timeZoneCountryMap = Maps.newTreeMap();
        // Optional set logging
        if (stormConf.get(CONF_LOGGING) != null) {
          isLogEnabled = (Boolean) stormConf.get(CONF_LOGGING);
        } else {
          isLogEnabled = false;
        }
        
        try {
			final URL url = TweetSentimentAnalyzerBolt.class.getResource("/time_zone_map.tsv");
			final String text = Resources.toString(url, Charsets.UTF_8);
			final Iterable<String> lineSplit = Splitter.on("\r").trimResults().omitEmptyStrings().split(text);
			List<String> csvSplit;
			for (final String str : lineSplit) {
				csvSplit = Lists.newArrayList(Splitter.on("\t").trimResults().omitEmptyStrings().split(str));
				if(csvSplit.size() > 1)
					timeZoneCountryMap.put(csvSplit.get(0), csvSplit.get(1));
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

    @Override
    /*
     * (non-Javadoc)
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     * "tweetId","user", "userHandle", "tweet",
                "time", "location", "country", "hashtags", "source",
                "lang", "retweetCount", "favoriteCount", "mentions", "longitude", "latidude","fulltext",
                "tokenType"
     */
    public void execute(Tuple input) {

    	long tweetId = input.getLongByField("tweetid");
    	String user = input.getStringByField("user");
		String userHandle = input.getStringByField("userhandle");
    	String tweet = (String) input.getValueByField("tweet");
    	Date createdDate =  (Date )input.getValueByField("time");
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
		
		if(null != timeZone && timeZone.length() > 0 ){
			String countryByTimeZone = timeZoneCountryMap.get(timeZone);
			
			if(countryByTimeZone != null && countryByTimeZone.length() > 0){
				country = countryByTimeZone;
			}
		}
		
    	
        //Remove all punctuation and new line chars in the tweet.
        
        DefaultTextTokenizer tokenizer = new DefaultTextTokenizer.Builder().
				setKeepPunctuation(false).build();
        
        TokenizedCharSequence tokSeq = tokenizer.tokenize(tweet);
        int tokenCnt = 0;
        Map<String,String> mapTokenTypeStem = new HashMap<String, String>();
        String token = "";
        for (Token tok : tokSeq.getTokens()) {
        	// trim the space.
        	token = tok.getTerm().toString().trim();
        	token = token.replaceAll("\\s+","");
        	
        	if(stopWords.contains(token))
				continue;
        	
        	String lemma = MorphaStemmer.stemToken(token);
        	lemma = lemma.trim().replaceAll("\n", "").replaceAll("\r", "");
        	
        	if(isLogEnabled){
        		LOG.trace(String.format("token %2d (%3d, %3d) type: %12s, token: '%s'", tokenCnt,
						tok.getOffset(), tok.getOffset() + tok.getLength(), tok.getType().name, lemma));
        	}
        	tokenCnt++;
			
        	mapTokenTypeStem.put(lemma, tok.getType().name);
			
		}
        
        collector.emit(input,new Values(tweetId,user, userHandle, tweet, createdDate, 
				location, country, hashtags, source, lang, 
				retweetCount, favoriteCount, mentions,
				longitude, latitude, fullText,
				mapTokenTypeStem, timeZone));
        
    }

    

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    	declarer.declare(new Fields("tweetid","user", "userhandle", "tweet",
                "time", "location", "country", "hashtags", "source",
                "lang", "retweetcount", "favoritecount", "mentions", "longitude", "latitude","fulltext",
                "tokentypeandstem", "timezone"));
     
    }
}
