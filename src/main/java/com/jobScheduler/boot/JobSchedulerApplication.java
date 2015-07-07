package com.jobScheduler.boot;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.jobScheduler.utility.DynamoDBUtilities;

@SpringBootApplication
@PropertySource("classpath:/app-config.properties")
@EnableConfigurationProperties
public class JobSchedulerApplication implements CommandLineRunner{
	
	private String mainTable = "MainTable";
	private String archivedTable = "ArchiveTable";
	private String mainTableKey = "Id";
	private String archivedTableKey = "Id";
	private Integer insertionInterval = 20;
	private Integer archivingInterval = 60;
	
	public static void main(String[] args) {
		SpringApplication.run(JobSchedulerApplication.class, args);
	}

	@Override
	public void run(String... arg0) throws Exception {
		new JobSchedulerApplication().AWSService();
	}
	
	@Bean
	public static PropertySourcesPlaceholderConfigurer propertyConfigInDev() {
		return new PropertySourcesPlaceholderConfigurer();
	}
	
	private void AWSService() throws Exception{
		
		DynamoDBUtilities.getDynamoDBInstance();
		//Creating Table
		DynamoDBUtilities.createTable(mainTable, mainTableKey);
		DynamoDBUtilities.createTable(archivedTable, archivedTableKey);
		
		//Using Thread 
		/*new InsertionThread(mainTable);
		new MasterArchivingThread(mainTable, archivedTable);*/
		
		//Using Quatrz
		DynamoDBUtilities.initaiteInsertionJob(insertionInterval, mainTable);
		DynamoDBUtilities.initaiteArchivingJob(archivingInterval, mainTable, archivedTable);*/
	}
	
	
}
