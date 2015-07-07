package com.jobScheduler.threadRunner;

import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.jobScheduler.utility.DynamoDBUtilities;

public class ArchiverThread implements Runnable {

	Thread thread;
	private String fromTableName;
	private String toTableName;
	
	public ArchiverThread(String fromTableName, String toTableName) {

		this.fromTableName = fromTableName;
		this.toTableName = toTableName;
		thread  = new Thread(this, toTableName);
		thread.start();
	}
	
	@Override
	public void run() {
		
		try{
			synchronized(thread){
				System.out.println("---Archiving Thread Initiated---");
				List<Map<String, AttributeValue>> items =
		        		DynamoDBUtilities.getItemsToBeArchived(fromTableName);

				if(items.size() >= 1 && items.size() <= 25){
					System.out.println("Performing Batch Write of " + items.size() + " elements");
					DynamoDBUtilities.batchWriteMapToTable(items, toTableName);
				} else {
					Integer limit = items.size() / 25;
					for(int i = 1; i <= limit; i++){
						List<Map<String, AttributeValue>> subItems =
								items.subList((i-1)*25, (i*25)-1);
						DynamoDBUtilities.batchWriteMapToTable(subItems, toTableName);
					}
					if(items.size() % 25 > 0){
						List<Map<String, AttributeValue>> subItems =
								items.subList(limit*25, (limit*25)-1);
						DynamoDBUtilities.batchWriteMapToTable(subItems, toTableName);
					}
				}
				for(Map<String, AttributeValue> item : items){
					DynamoDBUtilities.deleteItemFromTable(item, fromTableName);
				}
			}
		} catch(Exception e){
			e.printStackTrace();
		}
	}

}
