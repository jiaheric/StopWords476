package com.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;
/*
IntWritable is the Hadoop variant of Integer which has been optimized for serialization in the Hadoop environment.
An integer would use the default Java Serialization which is very costly in Hadoop environment.
see: https://stackoverflow.com/questions/52361265/integer-and-intwritable-types-existence
 */

public class StopWord {

    public static class MyMapperA
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            // count nb of lines in the file
            word = new Text("num_tweets");
            context.write(word, one);

        }

    }

    public static class MyReducerA
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();


        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        }
    }


    public static class MyMapperB
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            List<String> list_words = new ArrayList<String>();

            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                String w = word.toString();
                if (list_words.contains(w)) {
                    System.out.println("Word " + w + " already counted in the same document");
                } else {
                    list_words.add(w);
                    context.write(word, one);
                }
            }

        }
    }

    public static class MyReducerB
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum_words = 0;

            Configuration conf = context.getConfiguration();
            int param = conf.getInt("num_tweets", -1);

            for (IntWritable val : values) {
                sum_words += val.get();
            }

            if (sum_words >= param / 2) {
                result.set(sum_words);
                context.write(key, result);
            }
        }
    }

    public static class MyMapperC
            extends Mapper<Text, Text, IntWritable, Text> {

        private final static IntWritable frequency = new IntWritable();

        @Override
        public void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {

            int newVal = Integer.parseInt(value.toString());

            // for task Q2.1
//            if (newVal % 2 == 0) {
//                frequency.set(newVal);
//                context.write(frequency, key);
//            }

            // the group stage takes care of sorting in ascending order
            // if we want to get the words with the highest frequency (to simulate descending order), we need to multiply key with -1
            frequency.set(newVal * -1);
            context.write(frequency, key);

        }

    }


    public static class MyReducerC
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        static int count;

        public void setup(Context context) throws IOException, InterruptedException {
            count = 2; // top 2 most frequent stop words
        }
//        Text word = new Text();

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // for task Q2.1
//            for (Text value : values) {
//                word.set(value);
//                context.write(key, word);
//            }


            // the group stage takes care of sorting in ascending order
            // if we want to get the words with the highest frequency (to simulate descending order), we need to multiply key BACK with -1
            int nb_words = key.get() * -1;

            for (Text val : values) {
                String word = val.toString();

                // we just write top <count> records as output
                if (count > 0) {
                    context.write(new IntWritable(nb_words),
                            new Text(word));
                    count--;
                }

            }
        }

    }


    private static String outputPath;
    private static String inputPath;

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--input_path")) {
                inputPath = args[++i];
            } else if (args[i].equals("--output_path")) {
                outputPath = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line argument");
            }
        }

        if (outputPath == null || inputPath == null) {
            throw new RuntimeException("Either outputpath or input path are not defined");
        }


        try {
            Configuration conf = new Configuration();
            conf.set("mapreduce.job.queuename", "eecs476");         // required for this to work on GreatLakes
            conf.setInt("num_tweets", 10); // read from the output1 file

            //job 1
            Job j1 = Job.getInstance(conf, "j1");
            j1.setJarByClass(StopWord.class);
            j1.setMapperClass(MyMapperA.class);
            j1.setReducerClass(MyReducerA.class);
            j1.setMapOutputKeyClass(Text.class);
            j1.setMapOutputValueClass(IntWritable.class);
            j1.setOutputKeyClass(Text.class);
            j1.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(j1, new Path(inputPath));
            FileOutputFormat.setOutputPath(j1, new Path(outputPath + "1"));
            j1.waitForCompletion(true);

            //job 2
            Job j2 = Job.getInstance(conf, "j2");
            j2.setJarByClass(StopWord.class);
            j2.setMapperClass(MyMapperB.class);
            j2.setReducerClass(MyReducerB.class);
            j2.setMapOutputKeyClass(Text.class);
            j2.setMapOutputValueClass(IntWritable.class);
            j2.setOutputKeyClass(Text.class);
            j2.setOutputValueClass(IntWritable.class);
            j2.setInputFormatClass(TextInputFormat.class);
            j2.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(j2, new Path(inputPath));
            FileOutputFormat.setOutputPath(j2, new Path(outputPath + "2"));
            j2.waitForCompletion(true);

            //job 3
            Job j3 = Job.getInstance(conf, "j3");
            j3.setJarByClass(StopWord.class);
            j3.setMapperClass(MyMapperC.class);
            j3.setReducerClass(MyReducerC.class);
            j3.setMapOutputKeyClass(IntWritable.class);
            j3.setMapOutputValueClass(Text.class);
            j3.setOutputKeyClass(IntWritable.class);
            j3.setOutputValueClass(Text.class);
            j3.setInputFormatClass(KeyValueTextInputFormat.class);
            j3.setOutputFormatClass(TextOutputFormat.class);
            FileInputFormat.addInputPath(j3, new Path(outputPath + "2"));
            FileOutputFormat.setOutputPath(j3, new Path(outputPath + "3"));
            j3.waitForCompletion(true);


        } catch (Exception e) {
            System.err.println(e);
        }
    }

}
