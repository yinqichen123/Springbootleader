package edu.sjsu.cmpe172;

import org.springframework.boot.SpringApplication;        // Import the Spring Boot application startup class
import org.springframework.boot.autoconfigure.SpringBootApplication;    // Import Spring Boot auto-configuration annotations

@SpringBootApplication
public class LeaderElectionApplication {
    public static void main(String[] args) {
        // Application entry point
        SpringApplication.run(LeaderElectionApplication.class, args);
        // Start the Spring Boot application
    }
}
