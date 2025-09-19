package com.mobius.dt;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class FdApplication {

	public static void main(String[] args) {

        SpringApplication.run(FdApplication.class, args);
	}
}
