package org.hwx.demo.analytics.common;


/**
 * @author Nagaraj Jayakumar
 * 
 */
public enum StemPolarity {

	NEGATIVE("negative", -1),
	POSITIVE("positive", 1),
	NEUTRAL("neutral", 0),
	HAPPY("HAPPY",1),
	SAD("SAD",-1),
    UNKNOWN("UNKNOWN", 0),
    INSTANCE("INSTANCE", 0);

    private int  polarity;
    private String description;

    StemPolarity(String description, int  polarity) {
        this.description = description;
    	this.polarity = polarity;
    }

    public int geStemtPolarity(String stem) {
        for(StemPolarity sPolarity : StemPolarity.values()){
        	if(sPolarity.description.equalsIgnoreCase(stem)){
        		return sPolarity.polarity;
        	}
        }
    	return UNKNOWN.polarity;
    }
    
    public int getPolarity(){
    	return this.polarity;
    }
    
}
