package xyz.cjr.wiki_index;

/**
 * Created by cjr on 12/13/16.
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.wikiclean.WikiClean;
import org.wikiclean.WikiCleanBuilder;

public class IDOffsetTitle {

    public static String slice(String doc, String begin, String end) {
        int b = doc.indexOf(begin), e = doc.lastIndexOf(end);
        if (b < 0 || e < 0)
            return "";
        while (doc.charAt(b) != '>') b++;
        return doc.substring(b + 1, e);
    }

    public static String slice2(String doc, String begin, String end) {
        int b = doc.indexOf(begin), e = doc.indexOf(end);
        if (b < 0 || e < 0)
            return "";
        while (doc.charAt(b) != '>') b++;
        return doc.substring(b + 1, e);
    }

    public static class Map
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String doc = value.toString();
            if (slice(doc, "<text", "</text>").length() < 1)
                return;
            if (slice(doc, "<sha1>", "</sha1>").equals("t5mm8lyj25cvxe8i4i9pgokb6krbzj9"))
                return;

            String id = slice2(doc, "<id>", "</id>");
            if (id.length() < 1)
                return;
            String title = slice2(doc, "<title>", "</title>");
            String res = id.concat(" ").concat(key.toString()).concat(" ").concat(title);
            word.set(res);
            context.write(word, one);
        }
    }

    public static class Reduce
            extends Reducer<Text, IntWritable, Text, Text> {
        private Text result = new Text("");

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: IDOffsetTitle <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "IDOffsetTitle");
        job.setJarByClass(IDOffsetTitle.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

