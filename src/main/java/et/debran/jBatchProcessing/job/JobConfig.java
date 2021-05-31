package et.debran.jBatchProcessing.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JobConfig {
	
	@Bean
	Job processRecordsJob(@Qualifier("partitionedStep") Step partitionedStep, JobBuilderFactory jbf, JobCompletionListener listener) {
		return jbf.get("processRecords").incrementer(new RunIdIncrementer()).listener(listener)
				.start(partitionedStep)
				.build();
	}
	
}
