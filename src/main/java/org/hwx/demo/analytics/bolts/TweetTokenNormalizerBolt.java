package org.hwx.demo.analytics.bolts;

import java.io.File;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * normalization process verifies each token and 
 * performs some computing based on what kind of token it is 
 */
public class TweetTokenNormalizerBolt extends BaseRichBolt {

	/**
	 * Generated Serial Version UID
	 */
	private static final long serialVersionUID = 1745375745007882023L;

	public static final String ID = "TweetTokenNormalizerBolt";
	public static final String CONF_LOGGING = ID + ".logging";

	private OutputCollector collector;

	Logger LOG = LoggerFactory.getLogger(TweetTokenNormalizerBolt.class);

	private boolean isLogEnabled = false;

	private SpellChecker spellchecker;
	private List<String> filterTerms = Arrays.asList(new String[] { "http" });

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		// Optional set logging
		if (stormConf.get(CONF_LOGGING) != null) {
			isLogEnabled = (Boolean) stormConf.get(CONF_LOGGING);
		} else {
			isLogEnabled = false;
		}
		File dir = new File(System.getProperty("user.home") + "/dictionaries");
		Directory directory;
		try {
			directory = FSDirectory.open(dir);
			spellchecker = new SpellChecker(directory);
			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
			InputStream dictionaryFile = TweetTokenNormalizerBolt.class.getResourceAsStream("/dictionaries/fulldictionary00.txt");
			spellchecker.indexDictionary(new PlainTextDictionary(dictionaryFile), config, true);
		} catch (Exception e) {
			if(isLogEnabled)
				LOG.error(e.toString());
			// Should not occur. If it occurs, we cant continue. So, exiting at
			// this point itself.
			e.printStackTrace();
			System.exit(1);
		}

	

	}

	private boolean shouldKeep(String tokenType, String stem) {

		/*
		 * if it is token stem the check it in the dictionary
		 */
		if (!isTypeTokenStem(tokenType)) {
			/*
			 * if the token type is hashtag, username and emoticon just keep the
			 * tokenStem
			 */
			return isSpecialToken(tokenType);
		}
		if (stem == null)
			return false;
		if (stem.equals(""))
			return false;
		if (filterTerms.contains(stem))
			return false;
		// we don't want integers
		try {
			Integer.parseInt(stem);
			return false;
		} catch (Exception e) {
		}
		// or floating point numbers
		try {
			Double.parseDouble(stem);
			return false;
		} catch (Exception e) {
		}
		try {
			return spellchecker.exist(stem.toLowerCase());
		} catch (Exception e) {
			if(isLogEnabled)
				LOG.error(e.toString());
			return false;
		}
	}

	private boolean isTypeTokenStem(String tokenType) {

		if (TokenType.TOKEN.name.equalsIgnoreCase(tokenType)) {
			return true;
		}
		return false;
	}

	private boolean isSpecialToken(String tokenType) {
		TokenType[] types = TokenType.values();
		TokenType type = TokenType.TOKEN;
		for (TokenType indType : types) {
			if (indType.name.equalsIgnoreCase(tokenType)) {
				type = indType;
				break;
			}
		}
		boolean speacialToken = false;
		/*
		 * 
		 * TOKEN("token"), PUNCTUATION("punctation"), HASHTAG("hashtag"),
		 * USERNAME("username"), EMOTICON("emoticon"), URL("URL"), STOCK(
		 * "stock symbol"), CONTRACTION("contraction");
		 */
		switch (type) {
		case PUNCTUATION:
			speacialToken = false;
			break;

		case HASHTAG:
			speacialToken = true;
			break;

		case USERNAME:
			speacialToken = true;
			break;

		case EMOTICON:
			speacialToken = true;
			break;

		case URL:
			speacialToken = false;
			break;

		case STOCK:
			speacialToken = true;
			break;

		case CONTRACTION:
			speacialToken = true;
			break;

		default:
			speacialToken = false;
			break;
		}

		return speacialToken;
	}

	public boolean isKeep(String tokenType, 
						  String tokenStem ) {
		
		return shouldKeep(tokenType, tokenStem);
	}

	@SuppressWarnings("unchecked")
	@Override
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
		
		Map<String,String> mapTokenTypeStem =  (Map<String, String>) input.getValueByField("tokentypeandstem");
		Map<String,String> passedTokenTypeStem = new HashMap<>();
		
		
		for(String lemma:mapTokenTypeStem.keySet()){
			if (isKeep( mapTokenTypeStem.get(lemma), lemma)) {
				passedTokenTypeStem.put(lemma, mapTokenTypeStem.get(lemma));
			}
		}
		
		collector.emit(input,new Values(tweetId,user, userHandle, tweet, createdDate, 
				location, country, hashtags, source, lang, 
				retweetCount, favoriteCount, mentions,
				longitude, latitude, fullText,
				passedTokenTypeStem, timeZone));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("tweetid","user", "userhandle", "tweet",
                "time", "location", "country", "hashtags", "source",
                "lang", "retweetcount", "favoritecount", "mentions", "longitude", "latitude","fulltext",
                "tokentypeandstem", "timezone"));

	}
}
