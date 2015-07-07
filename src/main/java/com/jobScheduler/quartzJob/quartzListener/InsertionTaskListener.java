package com.jobScheduler.quartzJob.quartzListener;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;

public class InsertionTaskListener implements JobListener {

	public static final String TRIGGER_NAME = "InsertionTrigger";

	@Override
	public String getName() {

		return TRIGGER_NAME;
	}

	@Override
	public void jobToBeExecuted(JobExecutionContext context) {

		System.out.println("Job is going to be executed: "
				+ context.getJobDetail().getKey().toString());
	}

	@Override
	public void jobExecutionVetoed(JobExecutionContext context) {

		System.out.println("Job is vetoed by trigger: "
				+ context.getJobDetail().getKey().toString());
	}

	@Override
	public void jobWasExecuted(JobExecutionContext context,
			JobExecutionException jobException) {

		System.out.println("Exception thrown by: "
				+ context.getJobDetail().getKey().toString() + " Exception: "
				+ jobException.getMessage());
	}

}
