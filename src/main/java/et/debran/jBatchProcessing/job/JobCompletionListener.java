package et.debran.jBatchProcessing.job;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.stereotype.Component;


@Component
public class JobCompletionListener extends JobExecutionListenerSupport {
	Logger log = LoggerFactory.getLogger(JobCompletionListener.class);

	@Override
	public void afterJob(JobExecution jobExecution) {
		if(jobExecution.getStatus().equals(BatchStatus.COMPLETED)) {
			log.info("Job {} is completed successsfully and it tooks {} seconds ",
					jobExecution.getJobInstance().getJobName()
					,Duration.between(jobExecution.getStartTime().toInstant(), jobExecution.getEndTime().toInstant()).toSeconds()
					);
		}else {
			log.error("job {} exited with an error", jobExecution.getJobInstance().getJobName());
		}
		super.afterJob(jobExecution);
		
	}

	@Override
	public void beforeJob(JobExecution jobExecution) {
		
		log.info("starting job: {} ", jobExecution.getJobInstance().getJobName());
		super.beforeJob(jobExecution);
	}
	
	
}
