package com.jobScheduler.utility;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.Tables;
import com.jobScheduler.quartzJob.ArchivingJob;
import com.jobScheduler.quartzJob.InsertionJob;

public class DynamoDBUtilities {

	private static DynamoDB dynamoDB;
	private static AmazonDynamoDBClient dynamoDBClient;
	private static final Long timeInterval = 3*60L*1000L;
	
	private static AmazonDynamoDBClient getAmazonDynamoDBClient() throws Exception {

        try {
        	dynamoDBClient = new AmazonDynamoDBClient(
        			new ProfileCredentialsProvider("default").getCredentials());
        } catch (Exception e) {
            e.printStackTrace();
        }
        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
        dynamoDBClient.setRegion(usWest2);
        return dynamoDBClient;
    }
	
	private static DynamoDB initDynamoDb() throws Exception {

		DynamoDB dynamoDB = null;
		AmazonDynamoDBClient dynamoDBClient = getAmazonDynamoDBClient();
        dynamoDB = new DynamoDB(dynamoDBClient);
        return dynamoDB;
    }
	
	public static DynamoDB getDynamoDBInstance() throws Exception{
		
		if(dynamoDB == null){
			synchronized(AmazonDynamoDBClient.class){
				if(dynamoDB == null){
					dynamoDB = initDynamoDb();
				}
			}
		}
		return dynamoDB;
	}
	
	public static Boolean checkIfTableExist(String tableName){
		
		return Tables.doesTableExist(dynamoDBClient, tableName);
	}
	
	public static void createTable(String tableName, String key) throws InterruptedException{
	
		if(! checkIfTableExist(tableName)){
			ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
			attributeDefinitions.add(new AttributeDefinition().withAttributeName(key)
						.withAttributeType(ScalarAttributeType.N));
	
			ArrayList<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
			keySchema.add(new KeySchemaElement().withAttributeName(key).withKeyType(KeyType.HASH));
			
			CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
								.withKeySchema(keySchema)
								.withAttributeDefinitions(attributeDefinitions)
								.withProvisionedThroughput(new ProvisionedThroughput()
								    .withReadCapacityUnits(4L)
									.withWriteCapacityUnits(2L));
	
			Table table = dynamoDB.createTable(createTableRequest);
			
			System.out.println("Waiting for " + tableName + " to become ACTIVE...");
	        table.waitForActive();	
	        
			TableDescription createdTableDescription = dynamoDB.getTable(tableName).describe();
	        System.out.println("Created Table: " + tableName
	        		+ ", Description"+ createdTableDescription);
		} else{
			System.out.println("Table Already Exist.");
		}
	}
	
	public static Item convertToItem(Long id, String name, String timestamp,
			String rating, String body) {
        
		Item item = new Item()
				.withPrimaryKey("Id", id)
				.withString("Name", name)
				.withString("Timestamp", timestamp)
				.withString("Rating", rating)
				.withString("Body", body);
        return item;
    }
	
	public static Item convertMapToItem(Map<String, AttributeValue> map) {
        
		Item item = new Item()
				.withPrimaryKey("Id", Long.parseLong(map.get("Id").getN()))
				.withString("Name", map.get("Name").getS())
				.withString("Timestamp", map.get("Timestamp").getS())
				.withString("Rating", map.get("Rating").getS())
				.withString("Body", map.get("Body").getS());
        return item;
    }
	
	public static List<Map<String, AttributeValue>>
			getItemsToBeArchived(String tableName){
		
		Date twoMinutesAgo = new Date(new Date().getTime() - timeInterval);
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		String twoMinutesAgoStr = df.format(twoMinutesAgo);
		
		HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
        Condition condition = new Condition()
            .withComparisonOperator(ComparisonOperator.LT.toString())
            .withAttributeValueList(new AttributeValue().withS(twoMinutesAgoStr));
        scanFilter.put("Timestamp", condition);
        
        ScanRequest scanRequest = new ScanRequest()
        				.withTableName(tableName)
        				.withScanFilter(scanFilter);
        ScanResult scanResult = dynamoDBClient.scan(scanRequest);
        return scanResult.getItems();
	}
	
	public static void batchWriteMapToTable(List<Map<String, AttributeValue>> items, String tableName){
		
		List<Item> itemsList = new ArrayList<Item>();
		for(Map<String, AttributeValue> item : items){
			itemsList.add(convertMapToItem(item));
		}
		TableWriteItems forumTableWriteItems = new TableWriteItems(tableName)
				.withItemsToPut(itemsList);
		dynamoDB.batchWriteItem(forumTableWriteItems);
	}
	
	public static void putMapToTable(Map<String, AttributeValue> itemMap, String tableName){
		
		Item item = convertMapToItem(itemMap);
		putItemToTable(item, tableName);
	}

	public static void putItemToTable(Item item, String tableName){
		
		Table table = dynamoDB.getTable(tableName);
		table.putItem(item);
        System.out.println("+++Inserted Item: " + item.toJSONPretty());
	}
	
	public static void deleteItemFromTable(Map<String, AttributeValue> map, String tableName){
		
		Table table = dynamoDB.getTable(tableName);
		DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
				.withPrimaryKey("Id", Long.parseLong(map.get("Id").getN()));
		table.deleteItem(deleteItemSpec);
		System.out.println("***Deleted Item The Item with Id: " + map.get("Id").getN());
	}
	
	public static void initaiteInsertionJob(Integer insertionInterval,
			String tableName) throws SchedulerException{
		
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put("mainTable", tableName);
		    
		JobDetail insertionJob = JobBuilder.newJob(InsertionJob.class)
				.withIdentity("InsertionJob", "group1").build();
		
		Trigger insertionTrigger = TriggerBuilder.newTrigger()
				.withIdentity("InsertionTrigger", "group1")
				.usingJobData(jobDataMap)
				.withSchedule(SimpleScheduleBuilder.simpleSchedule()
				.withIntervalInSeconds(insertionInterval).repeatForever())
				.build();
		
		Scheduler insertionScheduler = new StdSchedulerFactory().getScheduler();
		insertionScheduler.start();
		insertionScheduler.scheduleJob(insertionJob, insertionTrigger);
	}
	
	public static void initaiteArchivingJob(Integer archivingInterval,
			String fromTableName, String toTableName)
			throws SchedulerException{
		
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put("fromTableName", fromTableName);
		jobDataMap.put("toTableName", toTableName);
		
		JobDetail archivingJob = JobBuilder.newJob(ArchivingJob.class)
				.withIdentity("ArchivingJob", "group2").build();
		
		Trigger archivingTrigger = TriggerBuilder.newTrigger()
				.withIdentity("ArchivingTrigger", "group2")
				.usingJobData(jobDataMap)
				.withSchedule(SimpleScheduleBuilder.simpleSchedule()
				.withIntervalInSeconds(archivingInterval).repeatForever())
				.build();
		
		Scheduler archivingScheduler = new StdSchedulerFactory().getScheduler();
		archivingScheduler.start();
		archivingScheduler.scheduleJob(archivingJob, archivingTrigger);
	}	
	
}
