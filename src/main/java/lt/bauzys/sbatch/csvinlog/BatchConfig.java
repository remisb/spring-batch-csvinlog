package lt.bauzys.sbatch.csvinlog;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final JobBuilderFactory jobs;
    private final StepBuilderFactory steps;
    private final DataSource dataSource;

    public BatchConfig(JobBuilderFactory jobs,
                       StepBuilderFactory steps,
                       DataSource dataSource) {
        this.jobs = jobs;
        this.steps = steps;
        this.dataSource = dataSource;
    }

    @Bean
    public FlatFileItemReader<Line> reader() {
        return new FlatFileItemReaderBuilder<Line>()
                .name("personItemReader")
                .resource(new ClassPathResource("input/data.csv"))
                .delimited()
                .names(new String[]{"name", "dob"})
                .fieldSetMapper(new LineFieldSetMapper())
                .build();
    }

    @Bean
    public LineItemProcessor processor() {
        return new LineItemProcessor();
    }

    @Bean
    public ListItemWriter<Line> writer() {
        return new ListItemWriter<Line>();
    }

    @Bean
    public JdbcBatchItemWriter<Line> jdbcWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Line>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .dataSource(dataSource)
                .sql("INSERT INTO person (name, dob, age) VALUES (:name, :dob, :age)")
//                .beanMapped()
                .build();
    }

    @Bean
    public Job logLinesJob(JobCompletionNotificationListener listener, Step step1) {
        return jobs.get("job1")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Step step1(JdbcBatchItemWriter<Line> writer) {
        return steps.get("step1")
                .<Line, Line>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer)
                .build();
    }
}
