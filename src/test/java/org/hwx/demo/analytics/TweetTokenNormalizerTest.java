package org.hwx.demo.analytics;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.spell.PlainTextDictionary;
import org.apache.lucene.search.spell.SpellChecker;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.hwx.demo.analytics.bolts.TweetTokenNormalizerBolt;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.common.text.DefaultTextTokenizer;
import com.twitter.common.text.token.TokenizedCharSequence;
import com.twitter.common.text.token.TokenizedCharSequence.Token;
import com.twitter.common.text.token.attribute.TokenType;

import backtype.storm.Testing;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.washington.cs.knowitall.morpha.MorphaStemmer;

/**
 * @author Nagaraj Jayakumar
 * Test class for the tweet token normalizer
 */
public class TweetTokenNormalizerTest {

	final static List<String> stopWords = Arrays.asList("a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
			"if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then",
			"there", "these", "they", "this", "to", "was", "will", "with");

	Logger LOG = LoggerFactory.getLogger(TweetTokenNormalizerBolt.class);

	private SpellChecker spellchecker;
	private List<String> filterTerms = Arrays.asList(new String[] { "http" });

	@Before
	public void prepare() {

		LOG.info(System.getProperty("user.home") + "/dictionaries" );
		File dir = new File(System.getProperty("user.home") + "/dictionaries");
		Directory directory;
		try {
			directory = FSDirectory.open(dir);
			spellchecker = new SpellChecker(directory);
			StandardAnalyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_36, analyzer);
			URL dictionaryFile = TweetTokenNormalizerBolt.class.getResource("/dictionaries/fulldictionary00.txt");
			spellchecker.indexDictionary(new PlainTextDictionary(new File(dictionaryFile.toURI())), config, true);
		} catch (Exception e) {
			LOG.error(e.toString());
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
			return spellchecker.exist(stem);
		} catch (Exception e) {
			e.printStackTrace();
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

	public boolean isKeep(Tuple tuple) {
		String tokenType = tuple.getString(0);
		String tokenStem = tuple.getString(1);

		return shouldKeep(tokenType, tokenStem);
	}

	@org.junit.Test
	public void testTweetTokenNormalizer() {

		try {
			Map<String, String> tokenAndType = getTokenAndTypeFromTweet();
			Tuple tuple = Testing.testTuple(new Values("emoticon", "hgf"));
			if (isKeep(tuple)) {
				System.out.println(tuple.toString());
			}else{
				System.out.println("Not present in dictionary " + tuple);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public Map<String, String> getTokenAndTypeFromTweet() {
		String tweet = "I 123: ran in a the to the what's #greater climbed's  a jolly fun :)".replace("\n", "")
				.replace("\r", "").replace("|", "").replace(",", "");

		System.out.println(tweet);
		Map<String, String> tokenAndType = new java.util.HashMap<>();
		DefaultTextTokenizer tokenizer = new DefaultTextTokenizer.Builder().setKeepPunctuation(false).build();

		TokenizedCharSequence tokSeq = tokenizer.tokenize(tweet);
		int tokenCnt = 0;
		for (Token tok : tokSeq.getTokens()) {
			if (stopWords.contains(tok.getTerm().toString()))
				continue;
			String lemma = MorphaStemmer.stemToken(tok.getTerm().toString());
			lemma = lemma.trim().replaceAll("\n", "").replaceAll("\r", "");

			System.out.println(String.format("token %2d (%3d, %3d) type: %12s, token: '%s'", tokenCnt, tok.getOffset(),
					tok.getOffset() + tok.getLength(), tok.getType().name, lemma));
			tokenCnt++;
			tokenAndType.put(tok.getType().name, lemma);
		}

		return tokenAndType;

	}
}
