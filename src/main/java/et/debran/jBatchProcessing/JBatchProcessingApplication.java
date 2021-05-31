package et.debran.jBatchProcessing;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class JBatchProcessingApplication {

	public static void main(String[] args) {
		SpringApplication.run(JBatchProcessingApplication.class, args);
	}

}
