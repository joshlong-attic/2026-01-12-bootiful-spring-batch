package com.example.batch;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.framework.ProxyFactoryBean;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.database.JdbcBatchItemWriter;
import org.springframework.batch.infrastructure.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingMessageHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.integration.core.GenericTransformer;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.file.dsl.Files;

import javax.sql.DataSource;
import java.io.File;

@SpringBootApplication
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

    @Bean
    JobLaunchingMessageHandler jobLaunchingMessageHandler(JobOperator jobOperator) {
        return new JobLaunchingMessageHandler(jobOperator);
    }

    @Bean
    IntegrationFlow integrationFlow(
            Job job,
            JobLaunchingMessageHandler jobLaunchingMessageHandler,
            @Value("file://${HOME}/Desktop/inbound") File csvDirectory) {
        var files = Files.inboundAdapter(csvDirectory).autoCreateDirectory(true);
        return IntegrationFlow
                .from(files)
                .transform((GenericTransformer<File, JobLaunchRequest>) source ->
                {
                    var jobParameters = new JobParametersBuilder()
                            .addString("file", source.getAbsolutePath())
                            .addString("name", "Enterprise Integration fans")
                            .toJobParameters();
                    return new JobLaunchRequest(job, jobParameters);
                })
                .handle(jobLaunchingMessageHandler)
                .get();
    }

/*
    @Bean
    ApplicationRunner jobRunner(
            NightlyReportJobConfiguration nightlyReportJobConfiguration,
            JobOperator jobOperator) {
        return _ -> {

            var job = nightlyReportJobConfiguration
                    .helloWorldJob(null, null, null);

            // for (var i = 0; i < 2; i++)
            jobOperator.start(job, new JobParametersBuilder()
                    .addString("name", "Vlad", false)
//                    .addString("uuid", UUID.randomUUID().toString())
                    .toJobParameters());

        };
    }
*/


}


@Configuration
class NightlyReportJobConfiguration {


    @Bean
    Step one(JobRepository repository) {
        return new StepBuilder(repository)
                .tasklet((contribution, _) -> {
                    var name = contribution.getStepExecution().getJobParameters()
                            .getString("name");
                    IO.println("setting up the world for " + name);
                    return RepeatStatus.FINISHED;
                })
                .allowStartIfComplete(true)
                .build();
    }


    record Dog(int id, String name, String description,
               String dob, char gender, String image
    ) {
    }

    @Bean
    AsyncTaskExecutor asyncTaskExecutor() {
        return new VirtualThreadTaskExecutor();
    }

    @Bean
    @StepScope
    FlatFileItemReader<Dog> dogFlatFileItemReader(
            @Value("#{stepExecution}") StepExecution stepExecution,
            @Value("#{stepExecution.jobParameters.getString('file')}") String filePath) {
        var file = stepExecution.getJobParameters().getString("file");
        IO.println("file: " + file);
        IO.println("reading from " + filePath);
        var actual = new FlatFileItemReaderBuilder<Dog>()
                .resource(new FileSystemResource(filePath))
                .saveState(true)
                .name("dogFlatFileItemReader")
                .delimited(d -> d
                        .names("id", "name", "description", "dob", "owner", "gender", "image")
                        .delimiter(","))
                .linesToSkip(1)
                .fieldSetMapper(fieldSet -> new Dog(
                        fieldSet.readInt("id"),
                        fieldSet.readString("name"),
                        fieldSet.readString("description"),
                        fieldSet.readString("dob"),
                        fieldSet.readChar("gender"),
                        fieldSet.readString("image")
                ))
                .build();

        return proxy("read", actual);
    }

    private static <T> T proxy(String log, T t) {
        var pfb = new ProxyFactoryBean();
        pfb.setTarget(t);
        pfb.setProxyTargetClass(true);
        pfb.addAdvice(new MethodInterceptor() {
            @Override
            public @Nullable Object invoke(MethodInvocation invocation) throws Throwable {
                IO.println(log + " " + Thread.currentThread());
                return invocation.getMethod().invoke(t, invocation.getArguments());
            }
        });
        return (T) pfb.getObject();

    }

    @Bean
    ItemProcessor<Dog, Dog> processor() {
        return new ItemProcessor<>() {
            @Override
            public @Nullable Dog process(Dog item) throws Exception {
                var c = Thread.currentThread();
                IO.println("processing " + c);
                if (item.id() == 101)
                    throw new IllegalStateException("couldn't continue!");
                return item;
            }
        };
    }

    @Bean
    JdbcBatchItemWriter<Dog> writer(DataSource dataSource) {
        return proxy("write", new JdbcBatchItemWriterBuilder<Dog>()
                .dataSource(dataSource)
                .sql("""
                        
                        insert into dogs (id, name, description, gender, image) 
                        values (
                            :id, :name, :description,:gender, :image
                        )
                        """)
                .beanMapped()
                /*    .itemPreparedStatementSetter(new ItemPreparedStatementSetter<Dog>() {
                        @Override
                        public void setValues(Dog item, PreparedStatement ps) throws SQLException {
                            ps.setInt(1, item.id());
                        }
                    })*/
                .build());

    }

    /*  @Bean
      ItemProcessor<Integer, String> processor() {
          return new ItemProcessor<Integer, String>() {
              @Override
              public @Nullable String process(Integer item) throws Exception {
                  return "";
              }
          };
      }
  */
  /*  @Bean
    ItemWriter<Dog> writer() {
        return chunk -> {
            IO.println("========");
            chunk.forEach(dog -> IO.println(dog.toString()));
            IO.println("========");
        };
    }
*/
    @Bean
    Step two(
            ItemReader<Dog> dogFlatFileItemReader,
            //FlatFileItemReader<Dog> dogFlatFileItemReader,
            ItemProcessor<Dog, Dog> processor, AsyncTaskExecutor asyncTaskExecutor,
            JdbcBatchItemWriter<Dog> jdbcBatchItemWriter,
            JobRepository repository) {
        return new StepBuilder(repository)
                .<Dog, Dog>chunk(5)
                .reader(dogFlatFileItemReader)
                .processor(processor)
                .writer(jdbcBatchItemWriter)
                .taskExecutor(asyncTaskExecutor)
                .allowStartIfComplete(true)

//                .skipPolicy(new SkipPolicy() {
//                    @Override
//                    public boolean shouldSkip(Throwable t, long skipCount) throws SkipLimitExceededException {
//                        return false;
//                    }
//                })

                .build();
    }


    @Bean
    Job helloWorldJob(JobRepository repository, Step one, Step two) {
        return new JobBuilder(repository)
                .start(one)
                .next(two)
//                .preventRestart()
                .incrementer(new RunIdIncrementer())
                .build();
    }

}


