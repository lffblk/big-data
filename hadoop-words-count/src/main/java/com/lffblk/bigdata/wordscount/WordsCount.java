package com.lffblk.bigdata.wordscount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by lffblk on 6/5/17.
 */
public class WordsCount {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        System.out.println("fs.default.name : - " + config.get("fs.default.name"));
        Path input = new Path("words-count");
        Path output = new Path("words-count-output");
        Job job = new Job(config, "wordscount");
        job.setJarByClass(WordsCount.class);
        job.setMapperClass(WordsCountMapper.class);
        job.setReducerClass(WordsCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
