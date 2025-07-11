package com.cap.dataAcquisition;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.client.RestTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

@SpringBootTest
@EnabledIf("com.cap.dataAcquisition.DataAcquisitionApplicationTests#isDockerAvailable")
class DataAcquisitionApplicationTests {

    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    @BeforeAll
    static void setup() {
        if (isDockerAvailable()) {
            postgres.start();
            System.setProperty("spring.datasource.url", postgres.getJdbcUrl());
            System.setProperty("spring.datasource.username", postgres.getUsername());
            System.setProperty("spring.datasource.password", postgres.getPassword());
        }
    }
    
    /**
     * Helper method to check if Docker is available
     * This is used by the @EnabledIf annotation to conditionally run tests
     */
    static boolean isDockerAvailable() {
        try {
            return DockerClientFactory.instance().isDockerAvailable();
        } catch (Exception e) {
            return false;
        }
    }

    @Test
    void contextLoads(ApplicationContext context) { //
        // This test verifies that the application context can start successfully
        assertNotNull(context);
    }

    @Test
    void main_shouldRunSpringApplication() {
        // Arrange
        String[] args = {"testArg"};
        try (MockedStatic<SpringApplication> mockedSpringApplication = Mockito.mockStatic(SpringApplication.class)) {
            mockedSpringApplication.when(() -> SpringApplication.run(DataAcquisitionApplication.class, args))
                                   .thenReturn(Mockito.mock(ConfigurableApplicationContext.class));

            // Act
            DataAcquisitionApplication.main(args); //

            // Assert
            mockedSpringApplication.verify(() -> SpringApplication.run(DataAcquisitionApplication.class, args), times(1));
        }
    }

    @Test
    void restTemplateBean_shouldBeCreated(ApplicationContext context) {
        // Act
        RestTemplate restTemplate = context.getBean(RestTemplate.class); //

        // Assert
        assertNotNull(restTemplate);
    }
}