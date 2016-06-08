package org.hwx.demo.analytics.topology;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.hwx.demo.analytics.bolts.TweetLoggerBolt;
import org.hwx.demo.analytics.bolts.TweetParserBolt;
import org.hwx.demo.analytics.bolts.TweetSentimentAnalyzerBolt;
import org.hwx.demo.analytics.bolts.TweetTokenNormalizerBolt;
import org.hwx.demo.analytics.bolts.TweetTokenizerBolt;
import org.hwx.demo.analytics.spouts.MockTweetsStreamSpout;
import org.hwx.demo.analytics.spouts.TweetsStreamSpout;
import org.hwx.demo.analytics.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.serializers.DefaultSerializers.TreeMapSerializer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author Nagaraj Jayakumar
 * 
 * This class is used to build the storm topology and Submits it Storm cluster
 *
 */
public class TweetTrendAndSentimentAnalyticsTopology {

	public static final String TOPOLOGY_NAME = "tweet-trend-sentiment-topology";

	public static final Logger LOG = LoggerFactory.getLogger(TweetTrendAndSentimentAnalyticsTopology.class);

	public static void main(String[] args) throws Exception {
		String serverMode = "runLocally";
		LOG.info("[USAGE] storm jar TwitterTrendAndSentimentAnalytics-0.0.1.jar "
				+ "org.hwx.demo.analytics.topology.TweetTrendAndSentimentAnalyticsTopology runOnServer [enableMockStream]");
		if (args.length > 0) {
			serverMode = args[0];
		}

		String isConnectToMockStream = "false";

		if (args.length > 1) {
			isConnectToMockStream = args[1];
		}

		String[] keyWords = new String[] {};
		Config conf = new Config();

		Properties prop = new Properties();
		InputStream input = null;

		// Read the topology specific config files.
		try {

			input = IOUtils.getInputStream("config.properties");
			// load a properties file
			prop.load(input);
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		// Create Spout
		IRichSpout spout = new MockTweetsStreamSpout(keyWords, 1000, (String) prop.get("ttsa.spout.filter.language"));
		String spoutID = MockTweetsStreamSpout.ID;

		boolean connectToMockStream = Boolean.parseBoolean(isConnectToMockStream);
		if (!connectToMockStream) {
			// Create Twitter stream spout
			spout = new TweetsStreamSpout(keyWords, 1000, (String) prop.get("ttsa.spout.filter.language"), true);
			spoutID = TweetsStreamSpout.ID;

		}

		// Create Bolts
		TweetParserBolt tweetParserBolt = new TweetParserBolt();
		TweetTokenizerBolt tweetTokenizerBolt = new TweetTokenizerBolt();
		TweetTokenNormalizerBolt tweetTokenNormalizerBolt = new TweetTokenNormalizerBolt();
		TweetSentimentAnalyzerBolt tweetSentimentAnalyzerBolt = new TweetSentimentAnalyzerBolt();
		TweetLoggerBolt tweetLoggerBolt = new TweetLoggerBolt();

		// Create Topology
		TopologyBuilder builder = new TopologyBuilder();

		// Set Spout
		builder.setSpout(spoutID, spout, Integer.parseInt(prop.getProperty("ttsa.spout.parallelism")));

		// Set Spout --> TweetParserBolt
		builder.setBolt(TweetParserBolt.ID, tweetParserBolt,
				Integer.parseInt(prop.getProperty("ttsa.bolt.tweetParserBolt.parallelism"))).shuffleGrouping(spoutID);

		// TweetParserBolt --> TweetTokenizerBolt
		builder.setBolt(TweetTokenizerBolt.ID, tweetTokenizerBolt,
				Integer.parseInt(prop.getProperty("ttsa.bolt.tweetTokenizerBolt.parallelism")))
				.shuffleGrouping(TweetParserBolt.ID);

		// TweetTokenizerBolt --> TweetTokenNormalizerBolt
		builder.setBolt(TweetTokenNormalizerBolt.ID, tweetTokenNormalizerBolt,
				Integer.parseInt(prop.getProperty("ttsa.bolt.tweetTokenNormalizerBolt.parallelism")))
				.shuffleGrouping(TweetTokenizerBolt.ID);

		// TweetTokenNormalizerBolt --> TweetSentimentAnalyzerBolt
		builder.setBolt(TweetSentimentAnalyzerBolt.ID, tweetSentimentAnalyzerBolt,
				Integer.parseInt(prop.getProperty("ttsa.bolt.tweetSentimentAnalyzerBolt.parallelism")))
				.shuffleGrouping(TweetTokenNormalizerBolt.ID);

		// TweetSentimentAnalyzerBolt --> TweetLoggerBolt
		builder.setBolt(TweetLoggerBolt.ID, tweetLoggerBolt,
				Integer.parseInt(prop.getProperty("ttsa.bolt.tweetLoggerBolt.parallelism")))
				.shuffleGrouping(TweetSentimentAnalyzerBolt.ID);

		HdfsBolt hdfsBolt = getHdfsBolt(prop);

		int hdfsBoltCount = 4;

		// define the topology grouping
		// TweetSentimentAnalyzerBolt --> hdfsBolt
		builder.setBolt("HDFSBolt", hdfsBolt, hdfsBoltCount).shuffleGrouping(TweetSentimentAnalyzerBolt.ID);

		HiveBolt hiveBolt = getHiveBolt(prop);

		// TweetSentimentAnalyzerBolt --> hiveBolt
		builder.setBolt("HIVEBOLT", hiveBolt, Integer.parseInt(prop.getProperty("ttsa.bolt.hiveBolt.parallelism")))
				.shuffleGrouping(TweetSentimentAnalyzerBolt.ID);

		// Set topology config
		conf.setNumWorkers(Integer.parseInt(prop.getProperty("ttsa.workers.num")));

		if (prop.get("ttsa.spout.max.pending") != null) {
			conf.setMaxSpoutPending(Integer.parseInt(prop.getProperty("ttsa.spout.max.pending")));
		}

		if (prop.get("ttsa.workers.childopts") != null) {
			conf.put(Config.WORKER_CHILDOPTS, prop.getProperty("ttsa.workers.childopts"));
		}
		if (prop.get("ttsa.supervisor.childopts") != null) {
			conf.put(Config.SUPERVISOR_CHILDOPTS, prop.getProperty("ttsa.supervisor.childopts"));
		}
	
		conf.put(TweetsStreamSpout.CONF_LOGGING,
				Boolean.parseBoolean(prop.getProperty("ttsa.spout.tweetsStreamSpout.logging")));

		conf.put(TweetParserBolt.CONF_LOGGING,
				Boolean.parseBoolean(prop.getProperty("ttsa.bolt.tweetParserBolt.logging")));

		conf.put(TweetTokenizerBolt.CONF_LOGGING,
				Boolean.parseBoolean(prop.getProperty("ttsa.bolt.tweetTokenizerBolt.logging")));

		conf.put(TweetTokenNormalizerBolt.CONF_LOGGING,
				Boolean.parseBoolean(prop.getProperty("ttsa.bolt.tweetTokenNormalizerBolt.logging")));

		conf.put(TweetSentimentAnalyzerBolt.CONF_LOGGING,
				Boolean.parseBoolean(prop.getProperty("ttsa.bolt.tweetSentimentAnalyzerBolt.logging")));

		conf.put(TweetLoggerBolt.CONF_LOGGING,
				Boolean.parseBoolean(prop.getProperty("ttsa.bolt.tweetLoggerBolt.logging")));

		conf.put(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION, false);
		conf.registerSerialization(TreeMap.class, TreeMapSerializer.class);

		if (serverMode.equalsIgnoreCase("runLocally")) {
			LOG.info("Running topology locally...");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
		} else {
			LOG.info("Running topology on cluster...");
			StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
		}
		LOG.info("To kill the topology run:");
		LOG.info("storm kill " + TOPOLOGY_NAME);
	}

	/**
	 * Method to get the hive bolt instance
	 * @param prop
	 * @return
	 */
	private static HiveBolt getHiveBolt(Properties prop) {
		String metaStoreURI = prop.getProperty("ttsa.bolt.hiveBolt.metaStoreURI");
		String dbName = prop.getProperty("ttsa.bolt.hiveBolt.dbName");
		String tblName = prop.getProperty("ttsa.bolt.hiveBolt.tblName");
		// Fields for possible column data
		String strColNames = prop.getProperty("ttsa.bolt.hiveBolt.colNames");

		String[] colNames = strColNames.split("\\s*,\\s*");
		// Record Writer configuration
		DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper().withColumnFields(new Fields(colNames))
				.withFieldDelimiter("\t");

		HiveOptions hiveOptions;
		hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper).withTxnsPerBatch(2).withBatchSize(1000)
				.withIdleTimeout(10).withCallTimeout(10000000);
		// .withKerberosKeytab(path_to_keytab)
		// .withKerberosPrincipal(krb_principal);
		return new HiveBolt(hiveOptions);
	}

	/**
	 * Method to get the HDFS bolt instance
	 * @param prop
	 * @return
	 */
	private static HdfsBolt getHdfsBolt(Properties prop) {
		// Setup HDFS bolt
		String rootPath = prop.getProperty("ttsa.bolt.hdfsBolt.rootPath");
		String prefix = prop.getProperty("ttsa.bolt.hdfsBolt.prefix");
		String fsUrl = prop.getProperty("ttsa.bolt.hdfsBolt.fsUrl");

		String strColNames = prop.getProperty("ttsa.bolt.hiveBolt.colNames");

		String[] colNames = strColNames.split("\\s*,\\s*");

		// this has to match the delimiter in the sql script that creates the
		// Hive table
		RecordFormat format = new DelimitedRecordFormat().withFields(new Fields(colNames)).withFieldDelimiter("|");

		// Synchronize data buffer with the filesystem
		SyncPolicy syncPolicy = new CountSyncPolicy(5);

		// Rotate data files when they reach five MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, Units.MB);

		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(rootPath + "/twitterdata")
				.withPrefix(prefix);

		// Instantiate the HdfsBolt
		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl(fsUrl).withFileNameFormat(fileNameFormat).withRecordFormat(format)
				.withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);
		return hdfsBolt;
	}

}