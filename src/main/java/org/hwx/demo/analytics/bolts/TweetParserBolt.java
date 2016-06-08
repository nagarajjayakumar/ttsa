package org.hwx.demo.analytics.bolts;


import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.UserMentionEntity;


/**
 * @author Nagaraj Jayakumar
 * TweetParserBolt will parse the tweet, extract fields out of it, 
 * and emit fields that can be processed by the next bolt in the topology.
 */
public class TweetParserBolt extends BaseRichBolt {

    /**
	 * 
	 */
	private static final long serialVersionUID = -4115085146419138532L;

	private OutputCollector collector;
    
    Logger LOG = LoggerFactory.getLogger(TweetParserBolt.class); 
    
    public static final String ID = "TweetParserBolt";
	public static final String CONF_LOGGING = ID + ".logging";
	private boolean isLogEnabled = false;
	
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        if (stormConf.get(CONF_LOGGING) != null) {
			isLogEnabled = (Boolean) stormConf.get(CONF_LOGGING);
		} else {
			isLogEnabled = false;
		}
    }

    @Override
    public void execute(Tuple input) {

        String user = null;
        String userHandle = null;

        String location = null;
        String country = null;
        List<String> hashtagList = new ArrayList<String>();
        List<String> mentionList = new ArrayList<String>();

        Status status = (Status) input.getValueByField("tweet");

        long tweetId = status.getId();
        
        //Remove all punctuation and new line chars in the tweet.
        String tweet = status.getText().replace("\n", "").replace("\r", "").replace("|","").replace(",","").replace("\t", "");
        
        String source = status.getSource().replace("|","");
        Date createdDate = status.getCreatedAt();
       
        HashtagEntity entities[] = status.getHashtagEntities();
        long retweetCount = status.getRetweetCount();
        long favoriteCount = status.getFavoriteCount();
        UserMentionEntity mentions[] = status.getUserMentionEntities();
        String lang = status.getLang();
        
        String longitude = "0";
		String latitude = "0";
		
		String fullText = status.toString().replace("\n", "").replace("\r", "").replace("|","");
        
        
        if(status.getGeoLocation()!=null){
        	longitude = Objects.toString(status.getGeoLocation().getLongitude(), "0");
        	latitude = Objects.toString(status.getGeoLocation().getLatitude(), "0");
		}

        // Extract hashtags
        if (entities != null) {
            for (HashtagEntity entity : entities) {
                String hashTag = entity.getText();
                hashtagList.add(hashTag);
            }
        }

        if (status.getPlace() != null) {
            if (status.getPlace().getName() != null) {
                location = status.getPlace().getName().replace("\n", "").replace("\r", "").replace("|","").replace(",","");
            }
            if (status.getPlace().getCountry() != null) {
                country = status.getPlace().getCountry().toUpperCase();
            }
        }
        
        if(country == null){
        	country = "default";
        }

        if (status.getUser() != null && status.getUser().getName() != null) {
            user = status.getUser().getName().replace("\n", "").replace("\r", "").replace("|","").replace(",","").replace("\t", "");
            userHandle = status.getUser().getScreenName().replace("\n", "").replace("\r", "").replace("|","").replace(",","");
        }

        if (mentions != null) {
            for (UserMentionEntity mention : mentions) {
                String mentionName = mention.getScreenName();
                mentionList.add(mentionName);
            }
        }

        String strHashtag = hashtagList.toString().replace("[", "").replace("]", "").replace("|","");
        String strUserMention = mentionList.toString().replace("[", "").replace("]", "").replace("|","");
        String timeZone = status.getUser().getTimeZone();
        
        // filter out non-English tweets
        if ("en".equalsIgnoreCase(lang)) {
        	if(isLogEnabled)
        		LOG.trace("Emitting : " + userHandle + " -> " + tweet);
            collector.emit(input, new Values(tweetId,user, userHandle, tweet, createdDate, 
            								location, country, strHashtag, source, lang, 
            								retweetCount, favoriteCount, strUserMention,
            								longitude, latitude, fullText, timeZone));
        }
    }

    

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    	declarer.declare(new Fields("tweetid","user", "userhandle", "tweet",
                "time", "location", "country", "hashtags", "source",
                "lang", "retweetcount", "favoritecount", "mentions", "longitude", "latitude","fulltext", "timezone"));
     
    }

}
