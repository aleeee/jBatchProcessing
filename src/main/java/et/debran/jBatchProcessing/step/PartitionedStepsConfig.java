package et.debran.jBatchProcessing.step;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import et.debran.jBatchProcessing.job.JobCompletionListener;
import et.debran.jBatchProcessing.model.Company;
import et.debran.jBatchProcessing.processor.StockProcessor;
import yahoofinance.histquotes.HistoricalQuote;

@Configuration
public class PartitionedStepsConfig {

	@Value("${chunk.size}")
	int chunkSize;
	@Value("${thread.size}")
	int threads;
	@Value("${output.dir}")
	String outputFile;
	
	@Autowired
	StepBuilderFactory sbf;
	
	@Autowired
	StockProcessor stockProcessor;
	
	@Autowired
	DataSource ds;
	
	@Scope("prototype")
	@Bean
	@StepScope
	public Partitioner jdbcPartitioner(String tableName, String columnName, 
			Map<String,Object> params,String whereCondition, NamedParameterJdbcTemplate jdbcTemplate) {
		return new ColumnRangePartitioner(jdbcTemplate, tableName, columnName, whereCondition, params);
		
	}
	
	
	@Bean
	@StepScope
	public ItemReader<Company> multithreadedJdbcReader(@Value("#{stepExecutionContext[minIndex]}") Long min,
			@Value("#{stepExecutionContext[maxIndex]}") Long max){
		Map<String, Object> params = new HashMap<>();
		params.put("minIndex", min);
		params.put("maxIndex", max);
		
		return new JdbcPagingItemReaderBuilder<Company>()
				.name("multi-thread-reader")
				.dataSource(ds)
				.selectClause(" select id, name, symbol ")
				.fromClause(" companies ")
				.whereClause("where id >= :minIndex and id <= :maxIndex")
				.parameterValues(params)
				.sortKeys(Collections.singletonMap("id", Order.ASCENDING))
				.rowMapper(new BeanPropertyRowMapper<>(Company.class))
				.build();
	}
	
	@Bean
	public ItemWriter<Company> jdbcWriter(){
		return new JdbcBatchItemWriterBuilder<Company>()
				.dataSource(ds)
				.beanMapped()
				.sql("update companies set status=:status where id =:id")
				.build();
	}
	
	
	@Bean
	public FlatFileItemWriter<HistoricalQuote> csvWriter(){
	    BeanWrapperFieldExtractor<HistoricalQuote> fieldExtractor = new BeanWrapperFieldExtractor<>();
	   	fieldExtractor.setNames(new String[] {"symbol", "date", "open", "low", "high","close","adjClose", "volume"});
		FlatFileItemWriter<HistoricalQuote> writer = new FlatFileItemWriterBuilder<HistoricalQuote>()
				.name("stock-writer")
				.delimited()
				.delimiter(";")
				.fieldExtractor(fieldExtractor)
				.lineAggregator(new DelimitedLineAggregator<HistoricalQuote>())
				.build();
		return writer;
	}
	
	
	
	@Bean
	public Step processRecords() {
		return sbf.get("processingStep").<Company,Company>chunk(chunkSize)
				.reader(multithreadedJdbcReader(null, null))
				.processor(stockProcessor)
				.writer(jdbcWriter())
				.build();
				
	}
	
	@Bean
	public Step partitionedStep(@Autowired NamedParameterJdbcTemplate jdbcTemplate) {
		return sbf.get("partitionedStep")
				.partitioner(processRecords().getName(), jdbcPartitioner("companies", "id", null, "", jdbcTemplate))
				.step(processRecords())
				.gridSize(threads)
				.taskExecutor(taskExecutor())
				.build();
	}
	
	@Bean
	public TaskExecutor taskExecutor() {
		ThreadPoolTaskExecutor executors = new ThreadPoolTaskExecutor();
		executors.setMaxPoolSize(threads);
		executors.setCorePoolSize(threads);
		return executors;
	}
		
	
	
	
}
