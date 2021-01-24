package com.example.tsvmerge;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class TsvmergeApplication {
	@Autowired
	TsvMerger tsvMerger;

	public static void main(String[] args) {
		try (ConfigurableApplicationContext ctx = SpringApplication.run(TsvmergeApplication.class, args)) {
			TsvmergeApplication app = ctx.getBean(TsvmergeApplication.class);
			app.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run(String... args) throws Exception {
		// @formatter:off
		List<File> files = Stream.of(args)
			.filter(s -> !s.startsWith("-"))
			.map(File::new)
			.collect(Collectors.toList());
		// @formatter:on

		tsvMerger.merge(files);
	}
}
