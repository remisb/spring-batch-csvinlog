package lt.bauzys.sbatch.csvinlog;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    private final JobBuilderFactory jobs;
    private final StepBuilderFactory steps;

    public BatchConfig(JobBuilderFactory jobs, StepBuilderFactory steps) {
        this.jobs = jobs;
        this.steps = steps;
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
    public Job logLinesJob(JobCompletionNotificationListener listener, Step step1) {
        return jobs.get("job1")
                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .flow(step1)
                .end()
                .build();
    }

    @Bean
    public Step step1() {
        return steps.get("step1")
                .<Line, Line>chunk(10)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }
}
