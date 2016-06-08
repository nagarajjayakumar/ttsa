package org.hwx.demo.analytics;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import com.twitter.common.text.DefaultTextTokenizer;
import com.twitter.common.text.token.TokenizedCharSequence;
import com.twitter.common.text.token.TokenizedCharSequence.Token;

/**
 * @author Nagaraj Jayakumar
 * Test Class for the tokenizer
 */
public class TweetTokenizerTest {
	final static List<String> stopWords = Arrays.asList(
		      "a", "an", "and", "are", "as", "at", "be", "but", "by",
		      "for", "if", "in", "into", "is", "it",
		      "no", "not", "of", "on", "or", "such",
		      "that", "the", "their", "then", "there", "these",
		      "they", "this", "to", "was", "will", "with"
		    );
	@org.junit.Test
	public void test() {
		List<String> mimeTypes = Arrays.asList(new String[] { "application/pdf", "text/html", "text/plain" });
		assertTrue(mimeTypes.contains("text/html"));

	}
	
	@org.junit.Test
	public void testTweetNormalizer(){
		String tweet = "I 123: ran in a the to the what's #greater climbed's  a jolly fun :)".replace("\n", "").replace("\r", "").replace("|","").replace(",","");
		
        System.out.println(tweet);
    	DefaultTextTokenizer tokenizer = new DefaultTextTokenizer.Builder().
				setKeepPunctuation(false).build();
        
        TokenizedCharSequence tokSeq = tokenizer.tokenize(tweet);
        int tokenCnt = 0;
        for (Token tok : tokSeq.getTokens()) {
        	if(stopWords.contains(tok.getTerm().toString()))
				continue;
			
			System.out.println(String.format("token %2d (%3d, %3d) type: %12s, token: '%s'", tokenCnt,
					tok.getOffset(), tok.getOffset() + tok.getLength(), tok.getType().name, tok.getTerm()));
			tokenCnt++;
		}
        
    
	}
}