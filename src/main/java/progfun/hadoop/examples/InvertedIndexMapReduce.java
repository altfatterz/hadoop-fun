package progfun.hadoop.examples;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public final class InvertedIndexMapReduce {
    public static void main(String... args) throws Exception {

        // Your input is 1 or more files, so create a sub-array from your input
        // arguments, excluding the last item of the array, which is the
        // MapReduce job output directory.

        runJob(Arrays.copyOfRange(args, 0, args.length - 1),
                args[args.length - 1]);
    }

    public static void runJob(String[] input, String output) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf);

        // The Job class setJarByClass method determines the JAR that contains the class that’s
        // passed-in, which beneath the scenes is copied by Hadoop into the cluster and subsequently
        // set in the Task’s classpath so that your Map/Reduce classes are available to the Task.
        job.setJarByClass(InvertedIndexMapReduce.class);

        // Set the Map class that should be used for the job
        job.setMapperClass(Map.class);
        // Set the Reduce class that should be used for the job.
        job.setReducerClass(Reduce.class);

        // If the map output key/value types differ from the input types you must tell Hadoop what
        // they are. In this case your map will output each word and file as the key/value pairs, and both are
        // Text objects.
        job.setMapOutputKeyClass(Text.class);

        // Set the map output value class
        job.setMapOutputValueClass(Text.class);

        Path outputPath = new Path(output);

        // Set the HDFS input files for your job. Hadoop expects multiple input files to be separated with commas
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        // Set the HDFS output directory for the job.
        FileOutputFormat.setOutputPath(job, outputPath);

        // Delete the existing HDFS output directory if it exists. If you don’t do this and the directory
        // already exists the job will fail.
        outputPath.getFileSystem(conf).delete(outputPath, true);

        // Tell the JobTracker to run the job and block until the job has completed.
        job.waitForCompletion(true);
    }

    /**
     * When you extend the MapReduce mapper class you specify the key/value types
     * for your inputs and outputs. You use the MapReduce default InputFormat for
     * your job, which supplies keys as byte offsets into the input file, and values as
     * each line in the file. Your map emits Text key/value pairs
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        // A Text object to store the document ID (filename) for your input.
        private Text documentId;

        // To cut down on object creation you create a single Text object, which you’ll reuse.
        private Text word = new Text();

        /*
        This method is called once at the start of the map and prior to the map method
        being called. You’ll use this opportunity to store the input filename for this map.
         */
        @Override
        protected void setup(Context context) {
            String filename =
                    ((FileSplit) context.getInputSplit()).getPath().getName();
            documentId = new Text(filename);
        }

        /*
        This map method is called once per input line; map tasks are run in parallel
        over subsets of the input files.
         */
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Your value contains an entire line from your file. You tokenize the line
            // using StringUtils (which is far faster than using String.split).
            for (String token : StringUtils.split(value.toString())) {
                word.set(token);
                // For each word your map outputs the word as the key and the document ID as the value.
                context.write(word, documentId);
            }
        }
    }

    /**
     * Much like your Map class you need to specify both the input and output key/value
     * classes when you define your reducer.
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private Text docIds = new Text();

        /*
        The reduce method is called once per unique map output key. The Iterable allows you
        to iterate over all the values that were emitted for the given key.
         */
        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {


            // Keep a set of all the document IDs that you encounter for the key.
            HashSet<Text> uniqueDocIds = new HashSet<Text>();

            // Iterate over all the DocumentIDs for the key.
            for (Text docId : values) {

                // Add the document ID to your set. The reason you create a new
                // Text object is that MapReduce reuses the Text object when
                // iterating over the values, which means you want to create a new copy.
                uniqueDocIds.add(new Text(docId));
            }
            docIds.set(new Text(StringUtils.join(uniqueDocIds, ",")));

            /*
            Your reduce outputs the word, and a CSV-separated list of document IDs that contained the word.
             */
            context.write(key, docIds);
        }
    }
}
