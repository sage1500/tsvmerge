package com.example.tsvmerge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TsvMerger {
    @Value("${encoding:UTF-8}")
    private String encoding;

    @Value("${columns:5}")
    private int columns;

    public void merge(List<File> files) throws Exception {
        log.info("merger: encoding={}", encoding);
        log.info("merger: columns={}", columns);
        log.info("merger: {}", files);

        if (files.isEmpty()) {
            return;
        }

        // @formatter:off
        TsvStream[] tsvs = files.stream()
            .map(f -> TsvStream.open(f, encoding))
            .collect(Collectors.toList())
            .toArray(new TsvStream[0]);
        // @formatter:on

        new MergeWork(tsvs, columns).mergeStreams();

        for (TsvStream tsv : tsvs) {
            tsv.close();
        }
    }

    @RequiredArgsConstructor
    private static class MergeWork {
        private final TsvStream[] tsvs;
        private final int columns;
        Set<Integer> marks;

        public void mergeStreams() {
            init();

            while (true) {
                // Mark
                mark();
                if (marks.isEmpty()) {
                    break;
                }

                // Output
                output();
            }
        }

        private void init() {
            marks = new HashSet<>();
        }

        private void mark() {
            String key = null;
            marks.clear();
            for (int i = 0; i < tsvs.length; i++) {
                TsvStream tsv = tsvs[i];

                String[] cols = tsv.peek();
                if (cols == null) {
                    continue;
                }

                String key2 = cols[0];
                if (key == null) {
                    key = key2;
                    marks.clear();
                    marks.add(i);
                    continue;
                }

                int d = key.compareTo(key2);
                if (d > 0) {
                    key = key2;
                    marks.clear();
                    marks.add(i);
                } else if (d == 0) {
                    marks.add(i);
                }
            }
        }

        private void output() {
            int nCol = 0;

            for (int i = 0; i < tsvs.length; i++) {
                if (!marks.contains(i)) {
                    continue;
                }

                TsvStream tsv = tsvs[i];
                String[] cols = tsv.poll();

                // Output Key
                if (nCol == 0) {
                    System.out.print(cols[0]);
                    nCol++;
                }

                // Output Padding Tabs
                int startCol = 1 + (columns - 1) * i;
                while (nCol < startCol) {
                    System.out.print("\t|");
                    nCol++;
                }

                // Output Cols
                for (int j = 1; j < cols.length; j++) {
                    if (j >= columns) {
                        break;
                    }

                    System.out.print("\t|");
                    System.out.print(cols[j]);
                    nCol++;
                }
            }
            System.out.println();
        }

    }

    @RequiredArgsConstructor
    private static class TsvStream implements AutoCloseable {
        final private BufferedReader reader;
        private String[] topCache = null;

        public static TsvStream open(File file, String encoding) {
            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), encoding));
                return new TsvStream(reader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws Exception {
            reader.close();
        }

        public String[] peek() {
            if (topCache == null) {
                topCache = read();
            }

            return topCache;
        }

        public String[] poll() {
            String[] result = peek();
            topCache = null;
            return result;
        }

        private String[] read() {
            while (true) {
                String line;
                try {
                    line = reader.readLine();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                if (line == null) {
                    return null;
                }

                String[] cols = line.split("\t");
                if (cols.length == 1 && cols[0].isEmpty()) {
                    continue;
                }

                return cols;
            }
        }

    }
}
