package xyz.cjr.wiki_index;

/**
 * Created by cjr on 12/16/16.
 */
import java.io.IOException;
import java.util.HashMap;
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

public class PageMaxWordCount {

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
            //String text = slice(doc, "<title>", "</title>").concat(" ")
            //    .concat(slice(doc, "<text", "</text>"));
            if (slice(doc, "<text", "</text>").length() < 1)
                return;
            if (slice(doc, "<sha1>", "</sha1>").equals("t5mm8lyj25cvxe8i4i9pgokb6krbzj9"))
                return;
            WikiClean cleaner = new WikiCleanBuilder().withTitle(true).build();
            String content = cleaner.clean(doc);
            char[] chs = content.toLowerCase().toCharArray();
            for (int i = 0; i < chs.length; i++) {
                int c = chs[i];
                if (!(('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')))
                    chs[i] = ' ';
            }

            String id = slice2(doc, "<id>", "</id>");
            if (id.length() < 1)
                return;

            StringTokenizer itr = new StringTokenizer(String.valueOf(chs));
            HashMap<String, Integer> HM = new HashMap<String, Integer>();
            String w;
            Integer t;
            while (itr.hasMoreTokens()) {
                w = itr.nextToken();
                if (null == HM.get(w)) t = 1;
                else t = HM.get(w) + 1;
                HM.put(w, t);
            }
            word.set(id);
            Integer ma = 0;
            for (Integer v : HM.values())
                if (ma < v) ma = v;
            context.write(word, new IntWritable(ma));

        }
    }

    public static class Reduce
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: PageMaxWordCount <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "PageMaxWordCount");
        job.setJarByClass(PageMaxWordCount.class);

        job.setInputFormatClass(XmlInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
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

