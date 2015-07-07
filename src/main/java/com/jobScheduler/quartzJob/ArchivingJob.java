package com.jobScheduler.quartzJob;

import java.util.List;
import java.util.Map;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.jobScheduler.utility.DynamoDBUtilities;

//Disallow running multiple jobs based on this class at the same time.
@DisallowConcurrentExecution	
public class ArchivingJob implements Job {

	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		String fromTableName = context.getTrigger().getJobDataMap().getString("fromTableName");
		String toTableName = context.getTrigger().getJobDataMap().getString("toTableName");
		
		try{
			System.out.println("---Archiving Thread Initiated---");
			List<Map<String, AttributeValue>> items =
	        		DynamoDBUtilities.getItemsToBeArchived(fromTableName);
	
			if(items.size() >= 1 && items.size() <= 25){
				System.out.println("Performing Batch Write of " + items.size() + " elements");
				DynamoDBUtilities.batchWriteMapToTable(items, toTableName);
			} else if(items.size() > 25) {
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
		} catch(Exception e){
			throw new JobExecutionException(e);
		}
	}

}
