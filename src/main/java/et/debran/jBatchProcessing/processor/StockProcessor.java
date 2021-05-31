package et.debran.jBatchProcessing.processor;

import java.util.Calendar;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;

import et.debran.jBatchProcessing.model.Company;
import yahoofinance.Stock;
import yahoofinance.YahooFinance;
import yahoofinance.histquotes.HistoricalQuote;
import yahoofinance.histquotes.Interval;

@Service
public class StockProcessor implements ItemProcessor<Company, Company> {
	Logger log = LoggerFactory.getLogger(StockProcessor.class);

	@Value("${output.dir}")
	String outputDir;

	private Calendar from;
	private Calendar to;
	private StepExecution stepExecution;

	@PostConstruct
	private void afterProperiesSet() {
		from = Calendar.getInstance();
		to = Calendar.getInstance();
		from.add(Calendar.DAY_OF_MONTH, -1);

	}

	@BeforeStep
	public void beforeStep(StepExecution stepExecution) {
		this.stepExecution = stepExecution;
	}

	@Override
	public Company process(Company company) throws Exception {
		try {
			Stock stock = YahooFinance.get(company.getSymbol(), from, to, Interval.WEEKLY);

			company.setStockHistory(stock.getHistory());
			FlatFileItemWriter<HistoricalQuote> writer = csvWriter(company.getName());
			writer.open(stepExecution.getExecutionContext());
			writer.write(company.getStockHistory());
			log.info("file written in {}.csv", outputDir + company.getName());
		} catch (Exception e) {
			log.error("error processing record: {}", e.getMessage());
		}
		return company;
	}

	public FlatFileItemWriter<HistoricalQuote> csvWriter(String filename) {
		BeanWrapperFieldExtractor<HistoricalQuote> fieldExtractor = new BeanWrapperFieldExtractor<>();
		fieldExtractor
				.setNames(new String[] { "symbol", "date", "open", "low", "high", "close", "adjClose", "volume" });
		FlatFileItemWriter<HistoricalQuote> writer = new FlatFileItemWriterBuilder<HistoricalQuote>()
				.name("stock-writer").resource(new FileSystemResource(outputDir + filename + ".csv")).delimited()
				.delimiter(";").fieldExtractor(fieldExtractor)
				.lineAggregator(new DelimitedLineAggregator<HistoricalQuote>()).build();
		return writer;
	}
}
