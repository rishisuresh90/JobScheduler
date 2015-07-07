package com.jobScheduler.threadRunner;


public class MasterArchivingThread implements Runnable {

	Thread thread;
	private String fromTableName;
	private String toTableName;
	private static final Long timeInterval = 2*60L*1000L;
	
	public MasterArchivingThread(String fromTableName, String toTableName) {

		this.fromTableName = fromTableName;
		this.toTableName = toTableName;
		thread  = new Thread(this, toTableName);
		thread.start();
	}
	
	@Override
	public void run() {
		while(true){
			try{
				synchronized(thread){
					System.out.println("---Archiving Thread Initiated---");
					new ArchiverThread(fromTableName, toTableName);
					Thread.sleep(timeInterval);
				}
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}

}
