package com.jobScheduler.quartzJob;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.jobScheduler.utility.DynamoDBUtilities;

//Disallow running multiple jobs based on this class at the same time.
@DisallowConcurrentExecution
public class InsertionJob implements Job {

	private static Long count = 1L;
	
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		
		try{
			String tableName = context.getTrigger().getJobDataMap().getString("mainTable");
			Date date = new Date();
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
			String timestamp = df.format(date);
			
			Item item = DynamoDBUtilities.convertToItem(
					count, "Name-" + count, timestamp, "Good", "Body - " + count);
			DynamoDBUtilities.putItemToTable(item, tableName);
			count++;
		} catch(Exception e){
			throw new JobExecutionException(e);
		}
	}

}
