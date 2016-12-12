package xyz.cjr.wiki_index;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class XmlInputFormat extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext context) {
        try {
            return new XMLRecordReader(inputSplit, context.getConfiguration());
        } catch (IOException e) {
            return null;
        }
    }
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        // TODO Auto-generated method stub
        return super.isSplitable(context, file);
    }


    public class XMLRecordReader extends RecordReader<LongWritable, Text> {
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private byte[] startTag;
        private byte[] endTag;
        private LongWritable currentKey;
        private Text currentValue;
        public static final String START_TAG_KEY = "xmlinput.start";
        public static final String END_TAG_KEY = "xmlinput.end";
        public XMLRecordReader() {
        }
        /**
         * 初始化读取资源以及相关的参数也可以放到initialize（）方法中去执行
         * @param inputSplit
         * @param context
         * @throws IOException
         */
        public XMLRecordReader(InputSplit inputSplit, Configuration context) throws IOException {
            /**
             * 获取开传入的开始和结束标签
             */
            startTag = context.get(START_TAG_KEY).getBytes("UTF-8");
            endTag = context.get(END_TAG_KEY).getBytes("UTF-8");
            FileSplit fileSplit = (FileSplit) inputSplit;
            /**
             * 获取分片的开始位置和结束的位置
             */
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(context);
            /**
             * 根据分片打开一个HDFS的文件输入流
             */
            fsin = fs.open(fileSplit.getPath());
            /**
             * 定位到分片开始的位置
             */
            fsin.seek(start);
        }
        @Override
        public void close() throws IOException {
            fsin.close();
        }
        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return currentKey;
        }
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return currentValue;
        }
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return fsin.getPos() - start / (float) end - start;
        }
        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext context)
                throws IOException, InterruptedException {
        /*startTag = context.getConfiguration().get(START_TAG_KEY).getBytes("UTF-8");
        endTag = context.getConfiguration().get(END_TAG_KEY).getBytes("UTF-8");
        FileSplit fileSplit = (FileSplit) inputSplit;
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        fsin = fs.open(fileSplit.getPath());
        fsin.seek(start);*/
        }
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            currentKey = new LongWritable();
            currentValue = new Text();
            return next(currentKey, currentValue);
        }
        private boolean next(LongWritable key, Text value) throws IOException {
            /**
             *  通过readUntilMatch方法查找xml段开始的标签，直到找到了，才开始
             *  写xml片段到buffer中去，如readUntilMatch的第二个参数为false则不查找的过
             *  程中写入数据到buffer，如果为true的话就边查找边写入
             */
            if( fsin.getPos() < end && readUntilMatch(startTag, false)) {
                //进入代码段则说明找到了开始标签，现在fsin的指针指在找到的开始标签的
                //最后一位上，所以向buffer中写入开始标签
                buffer.write(startTag);
                try {
                    /**
                     * 在fsin中去查找结束标签边查找边记录直到找到结束标签为止
                     */
                    if(readUntilMatch(endTag, true)) {
                        /**
                         * 找到标签后把结束标签的指针位置的偏移量赋值给key
                         * 把buffer中记录的整个xml完整片断赋值给value
                         */
                        key.set(fsin.getPos());
                        value.set(buffer.getData(), 0, buffer.getLength());
                        return true;
                    }
                } finally {
                    buffer.reset();
                }
            }
            return false;
        }
        /**
         * 读取xml文件匹配标签的方法
         * @param startTag
         * @param isWrite
         * @return
         * @throws IOException
         */
        private boolean readUntilMatch(byte[] startTag, boolean isWrite) throws IOException {
            int i = 0;
            while(true) {
                /**
                 * 从输入文件只读取一个Byte的数据
                 */
                int b = fsin.read();
                if( b == -1) {
                    return false;
                }
                /**
                 *  如果在查找开始标签则不记录查找过程，
                 *  在查找结束标签时才记录查找过程。
                 */
                if(isWrite) {
                    buffer.write(b);
                }
                /**
                 * 判断时否找到指定的标签来判断函数结束的时间点
                 */
                if(b == startTag[i]) {
                    i ++;
                    if( i >= startTag.length) {
                        return true;
                    }
                } else {
                    i = 0;
                }
                // see if we've passed the stop point:
                if (!isWrite && i == 0 && fsin.getPos() >= end) {
                    return false;
                }
            }
        }
    }
}

