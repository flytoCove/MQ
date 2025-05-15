package com.fly.mq;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.ConfigurableBootstrapContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class MqApplication {

    public static ConfigurableApplicationContext context = null;
    public static void main(String[] args) {
        context = SpringApplication.run(MqApplication.class, args);
    }

}
