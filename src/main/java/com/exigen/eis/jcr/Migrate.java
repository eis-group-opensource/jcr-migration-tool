/* Copyright © 2016 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.exigen.eis.jcr;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Migrate {

	private final static Logger LOG = LoggerFactory.getLogger(Migrate.class);

	public static void main(String[] args) throws Exception {

		
		LOG.info("JCR to CMIS migration started.");
		long startDate = System.currentTimeMillis();
		Export.main(null);
		Import.main(null);
		LOG.info("JCR to CMIS migration finished in " + getDurationBreakdown((System.currentTimeMillis() - startDate)));
	}

    
	public static String getDurationBreakdown(long millis) {
    	if(millis < 0) {
            throw new IllegalArgumentException("Duration must be greater than zero!");
        }
        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        millis -= TimeUnit.HOURS.toMillis(hours);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
        millis -= TimeUnit.MINUTES.toMillis(minutes);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
        millis -= TimeUnit.SECONDS.toMillis(seconds);

        StringBuilder sb = new StringBuilder(64);
 
        if (hours != 0) {
        	sb.append(hours);
        	sb.append(" h ");
        }
        if (minutes != 0){
        	sb.append(minutes);
        	sb.append(" m ");
        }
        sb.append(seconds);
        sb.append(" s ");
        sb.append(millis);
        sb.append(" ms");
        
        return(sb.toString());
    }
}
