package com.jobScheduler.threadRunner;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.jobScheduler.utility.DynamoDBUtilities;

public class InsertionThread implements Runnable {

	Thread thread;
	private String tableName;
	private static Long count = 1L;
	private static final Long timeInterval = 20L*1000L;
	
	public InsertionThread(String tableName) {

		this.tableName = tableName;
		thread  = new Thread(this, tableName);
		thread.start();
	}
	
	@Override
	public void run() {
		while(true){
			try{
				synchronized(thread){
					Date date = new Date();
					SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
					String timestamp = df.format(date);
					
					Item item = DynamoDBUtilities.convertToItem(
							count, "Name-" + count, timestamp, "Good", "Body - " + count);
					DynamoDBUtilities.putItemToTable(item, tableName);
					Thread.sleep(timeInterval);
				}
			} catch(Exception e){
				e.printStackTrace();
			}
			count++;
		}
	}

}
