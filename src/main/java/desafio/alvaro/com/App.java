package desafio.alvaro.com;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;


public class App {

	public static void main(String[] args) {

		try {
			//Instancia o Job
			JobDetail job = JobBuilder.newJob(FileProcessorJob.class)
					.withIdentity("fileProcessorJob")
					.build();
			
			//Agenda o Job para rodar de 3 em 3 minutos
			Trigger trigger = TriggerBuilder.newTrigger().withSchedule(SimpleScheduleBuilder
																		.simpleSchedule()																	
																		.withIntervalInMinutes(3)
																		.repeatForever()).build();
			
			SchedulerFactory schFactory = new StdSchedulerFactory();
			Scheduler sch;
			sch = schFactory.getScheduler();
			sch.start();
			
			sch.scheduleJob(job, trigger);
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}
	
	
}
