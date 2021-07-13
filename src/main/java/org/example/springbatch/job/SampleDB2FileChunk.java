package org.example.springbatch.job;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.stereotype.Component;
import javax.sql.DataSource;

@Component
public class SampleDB2FileChunk {

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    private Resource outputResource = new FileSystemResource("data/output/employee.txt");

    @Bean
    public Job sampleDb2FileChunkJob(SampleJobListener jobListener, Step sampleDb2FileChunkStep) {
        return jobBuilderFactory.get("sampleDb2FileChunkJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobListener)
                .flow(sampleDb2FileChunkStep)
                .end()
                .build();
    }
    @Bean
    public Step sampleDb2FileChunkStep(JdbcCursorItemReader<Employee> jdbcCursorItemReader, SampleStepListener stepListener, FlatFileItemWriter<Employee> flatFileItemWriter) {
        return stepBuilderFactory.get("sampleDb2FileChunkStep")
                .listener(stepListener)
                .<Employee, Employee> chunk(10)
                .reader(jdbcCursorItemReader)
                .processor(sampleDb2FileChunkProcessor())
                .writer(flatFileItemWriter)
                .build();
    }
    @Bean
    public JdbcCursorItemReader<Employee> jdbcCursorItemReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<Employee>()
                .name("jdbcCursorItemReader")
                .fetchSize(100)
                .dataSource(dataSource)
                .rowMapper(new BeanPropertyRowMapper<>(Employee.class))
                .sql("SELECT user_id, user_name, user_gender, department_code FROM BATCH_SAMPLE_EMPLOYEE")
                .build();
    }
    @Bean
    public EmployeeProcessor sampleDb2FileChunkProcessor() {
        return new EmployeeProcessor();
    }
    @Bean
    public FlatFileItemWriter<Employee> flatFileItemWriter() {
        FlatFileItemWriter<Employee> writer = new FlatFileItemWriter<>();
        writer.setResource(outputResource);
        writer.setAppendAllowed(true);
        writer.setLineAggregator(new DelimitedLineAggregator<Employee>() {
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<Employee>() {
                    {
                        setNames(new String[] { "userId", "userName", "userGender", "departmentCode" });
                    }
                });
            }
        });
        return writer;
    }

}
