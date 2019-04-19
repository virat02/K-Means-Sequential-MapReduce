package seqkmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class KMeansSeq extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(KMeansSeq.class);

    public static class KMeansSeqMapper extends
            Mapper<Object, Text, Text, Text> {

        private List<ArrayList<Double>> records = new ArrayList<>();

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);

            //Retrieve from local cache
            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {

                //Read input to a bufferedReader
                BufferedReader br = new BufferedReader((new FileReader("normalized_data.csv")));

                String line;
                while ((line = br.readLine()) != null) {
                    String[] keyvalue = line.split(";");

                    ArrayList<Double> record = new ArrayList<>();
                    for (String s : keyvalue) {
                        record.add(Double.parseDouble(s));
                    }
                    records.add(record);
                }

                br.close();

            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            System.out.println(records);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {

        int val;
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "K Mean Sequential");
        job.setJarByClass(KMeansSeq.class);

        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\n");

        //Add the input to cache file
        job.addCacheFile((new Path("input/normalized_data.csv")).toUri());

        //set the sequential flow of job
        job.setMapperClass(KMeansSeqMapper.class);
        job.setNumReduceTasks(0);

        //Set the input path for job
        TextInputFormat.setInputPaths(job, new Path(args[0]));

        //Set the output path for job
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        //Set the output key and value type for this job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        val = job.waitForCompletion(true) ? 0 : 1;

        return val;
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
        }

        try {
            ToolRunner.run(new KMeansSeq(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
