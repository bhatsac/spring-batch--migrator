package com.migrator.batch.config;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.migrator.batch.mapper.CustomerRowMapper;
import com.migrator.batch.model.Customer;
import com.migrator.batch.partitioner.ColumnRangePartitioner;

@Configuration
public class JobConfiguration 
{
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Bean
	public ColumnRangePartitioner partitioner() 
	{
		ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();
		columnRangePartitioner.setColumn("id");
		columnRangePartitioner.setDataSource(dataSource);
		columnRangePartitioner.setTable("customer");
		return columnRangePartitioner;
	}

	@Bean
	@StepScope
	public JdbcPagingItemReader<Customer> pagingItemReader(
			@Value("#{stepExecutionContext['minValue']}") Long minValue,
			@Value("#{stepExecutionContext['maxValue']}") Long maxValue) 
	{
		System.out.println("reading------- " + minValue + " to--------- " + maxValue);

		Map<String, Order> sortKeys = new HashMap<>();
		sortKeys.put("id", Order.ASCENDING);
		
		MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
		queryProvider.setSelectClause("id, firstName, lastName, birthdate");
		queryProvider.setFromClause("from customer");
		queryProvider.setWhereClause("where id >= " + minValue + " and id <= " + maxValue);
		queryProvider.setSortKeys(sortKeys);
		
		JdbcPagingItemReader<Customer> reader = new JdbcPagingItemReader<>();
		reader.setDataSource(this.dataSource);
		reader.setFetchSize(100);
		reader.setRowMapper(new CustomerRowMapper());
		reader.setQueryProvider(queryProvider);
		
		return reader;
	}
	
	
	@Bean
	@StepScope
	public JdbcBatchItemWriter<Customer> customerItemWriter()
	{
		JdbcBatchItemWriter<Customer> itemWriter = new JdbcBatchItemWriter<>();
		itemWriter.setDataSource(dataSource);
		itemWriter.setSql("INSERT INTO NEW_CUSTOMER VALUES (:id, :firstName, :lastName, :birthdate)");

		itemWriter.setItemSqlParameterSourceProvider
			(new BeanPropertyItemSqlParameterSourceProvider<>());
		itemWriter.afterPropertiesSet();
		  System.out.println("---<><><><><><><><>"+ Thread.currentThread().getName());
		
		return itemWriter;
	}
	
	@Bean
	@StepScope
	public ItemProcessor<Customer,Customer> customerProcessor()
	{
		ItemProcessor<Customer,Customer> itemProcessor=(Customer item)-> {
			System.out.println(item.toString()+"<---------------------------------");
			if(item.getId()==18 || item.getId()==1){
				System.out.println("RuntimeException"+"<----------"+ item);
				throw new RuntimeException("I am gone!");
				
			}
			return item;
		};
		return itemProcessor;
	}
	
	// Master
	@Bean
	public Step step1() 
	{
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setCorePoolSize(2);
		threadPoolTaskExecutor.setMaxPoolSize(2);
		threadPoolTaskExecutor.setThreadNamePrefix("Part_thread_");
		threadPoolTaskExecutor.initialize();
		return stepBuilderFactory.get("step1")
				.partitioner(slaveStep().getName(), partitioner())
				.step(slaveStep())
				.gridSize(5)
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}
	
	// slave step
	@Bean
	public Step slaveStep() 
	{
		ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
		threadPoolTaskExecutor.setCorePoolSize(2);
		threadPoolTaskExecutor.setMaxPoolSize(2);
		threadPoolTaskExecutor.setThreadNamePrefix("chunk_thread_");
		threadPoolTaskExecutor.initialize();
		return stepBuilderFactory.get("slaveStep")
				.<Customer, Customer>chunk(1)
				.reader(pagingItemReader(null, null))
				.processor(customerProcessor())
				.writer(customerItemWriter())
				.faultTolerant()
				//.retry(RuntimeException.class)
				//.retryLimit(5)
				.skip(RuntimeException.class)
				.skipLimit(100)	
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}
	
	@Bean
	public Job job() 
	{
		return jobBuilderFactory.get("job")
				.start(step1())
				.build();
	}
}