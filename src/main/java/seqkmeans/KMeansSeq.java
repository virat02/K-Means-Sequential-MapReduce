package seqkmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
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

        private static List<ArrayList<Double>> records = new ArrayList<>();
        private static ArrayList<ArrayList<Double>> centroids = new ArrayList<>();

        //Threshold for convergence(set to 1%)
        private static final double THRESHOLD = 0.01;

        //Initialize the termination to false
        private static boolean runIteration = false;

        //private static HashMap<ArrayList<Double>, Double> centroidMap = new HashMap<>();
        private static HashMap<ArrayList<Double>, ArrayList<ArrayList<Double>>> clusterMap = new HashMap<>();

        //Euclidean distance
        private static Double distance(ArrayList<Double> centroid, ArrayList<Double> record) {
            double sum = 0.0;
            int size = centroid.size();
            // ignoring the last elememt ... which is the actual label for now
            for (int i = 0; i < size - 1; i++) {
                sum += Math.pow(centroid.get(i) - record.get(i), 2);
            }
            return Math.sqrt(sum);
        }

        private static void selectRandomCentroids(int k) {
            // Selecting k centroids at random
            // Reference - https://stackoverflow.com/questions/12487592/randomly-select-an-item-from-a-list
            int i = 0;
            while (i < k){
                Random random = new Random();
                ArrayList<Double> centroid = records.get(random.nextInt(records.size()));
                centroids.add(centroid);
                i++;

            }
        }

        private static void evaluateClusterMap() {

            clusterMap.clear();

            // creating k entries in hashmap, one for each centroid
            for (ArrayList<Double> c: centroids) {
                clusterMap.put(c, new ArrayList<>());
            }
        }

        private static ArrayList<Double> initializeList(int size) {
            ArrayList<Double> a = new ArrayList<>();
            for(int j = 0; j < size; j++){
                a.add(0.0);
            }

            return a;
        }

        private static void evaluateCentroids(){

            //CLear the previous centroids
            centroids.clear();

            for (ArrayList<Double> c: clusterMap.keySet()) {

                //create initial centroid
                ArrayList<Double> new_c = initializeList(clusterMap.get(c).get(0).size());

                for (ArrayList<Double> r: clusterMap.get(c)) {
                    //start from first column
                    int i = 0;

                    //loop until last column, each column denotes one feature
                    //ignore the last column which is the feature: label
                    while (i < r.size() - 1){

                        //calculate the average of the column under consideration
                        new_c.set(i, new_c.get(i) + (r.get(i)/clusterMap.get(c).size()));
                        i++;
                    }
                }

                //check for convergence
                if(distance(c,new_c) / distance(initializeList(c.size()), c) > THRESHOLD) {
                    runIteration = true;
                }
                centroids.add(new_c);
            }
        }

        private static void kMeans(){
            // running k means

            //Run the iteration based on the flag set during convergence
            do {
                runIteration = false;
                for (ArrayList<Double> r : records) {

                    Double min_val = Double.MAX_VALUE;
                    ArrayList<Double> selected_c = null;
                    Double d;

                    for (ArrayList<Double> c : centroids) {
                        d = distance(c, r);
                        if (d < min_val) {
                            selected_c = c;
                            min_val = d;
                        }
                    }
                    clusterMap.get(selected_c).add(r);
                }

                //To recompute new centroids by averaging the records assigned to each
                //Sets the flag to false if converged
                evaluateCentroids();

                if(runIteration){
                    evaluateClusterMap();
                }


            } while(runIteration);
        }

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

            selectRandomCentroids(Integer.parseInt(key.toString()));
            context.write(null, new Text("Random centroids"));
            context.write(null,new Text(centroids.toString()));
            evaluateClusterMap();
            kMeans();

            for (ArrayList<Double> k: clusterMap.keySet()) {
                context.write(null, new Text("----------------------"));
                context.write(null, new Text(k.toString()));
                context.write(null, new Text("-----------------------"));
                for (ArrayList<Double> r: clusterMap.get(k)) {
                    context.write(null, new Text("\n"));
                    context.write(null, new Text((r.get((r.size() - 1))).toString()));
                    context.write(null, new Text("\n"));
                }
            }
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
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(args[2]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 1);

        //Set the output path for job
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        //Set the output key and value type for this job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        val = job.waitForCompletion(true) ? 0 : 1;

        return val;
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 3) {
        }

        try {
            ToolRunner.run(new KMeansSeq(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }
}
