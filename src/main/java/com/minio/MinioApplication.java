package com.minio;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.github.drinkjava2.jsqlbox.DbContext;
import com.github.drinkjava2.jtransactions.spring.SpringTxConnectionManager;

@SpringBootApplication
public class MinioApplication {
	@Autowired
	DataSource ds; 
	public static void main(String[] args) {
		SpringApplication.run(MinioApplication.class, args);
	}
	@Bean
	public DbContext createDefaultDbContext() {
		DbContext ctx = new DbContext(ds);
		ctx.setConnectionManager(SpringTxConnectionManager.instance() );
		ctx.setAllowShowSQL(false);
		DbContext.setGlobalDbContext(ctx);// 设定静态全局上下文
		return ctx;
	}
}
