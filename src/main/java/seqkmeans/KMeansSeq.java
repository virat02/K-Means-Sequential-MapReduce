package seqkmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
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
        private static List<ArrayList<Double>> centroids;
        private static List<ArrayList<Double>> newCentroids;
        private static List<Integer> recordsInCentroid;

        //Threshold for convergence(set to 1%)
        private static final double THRESHOLD = 0.01;

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

            centroids = new ArrayList<>();
            int i = 0;
            while (i < k){
                Random random = new Random();
                ArrayList<Double> centroid = records.get(random.nextInt(records.size()));
                centroids.add(centroid);
                i++;

            }
        }

        private static void initializeNewCentroids() {

            newCentroids = new ArrayList<>();
            recordsInCentroid = new ArrayList<>();

            // creating k entries in hashmap, one for each centroid
            for (ArrayList<Double> c: centroids) {
                newCentroids.add(initializeList(c.size()));
                recordsInCentroid.add(0);
            }
        }

        private static ArrayList<Double> initializeList(int size) {
            ArrayList<Double> a = new ArrayList<>();
            for(int j = 0; j < size; j++){
                a.add(0.0);
            }

            return a;
        }

        private static void kMeans(){
            // running k means
            //Initialize the termination to false
            boolean runIteration;

            //Run the iteration based on the flag set during convergence
            do {
                runIteration = false;
                initializeNewCentroids();
                for (ArrayList<Double> r : records) {

                    Double min_val = Double.MAX_VALUE;
                    int selectedCentroidNum = 0;
                    Double d;

                    int i = 0;
                    while (i < centroids.size()) {
                        d = distance(centroids.get(i), r);
                        if (d < min_val) {
                            selectedCentroidNum = i;
                            min_val = d;
                        }
                        i++;
                    }
                    for (int z = 0; z < r.size() - 1; z++){
                        newCentroids.get(selectedCentroidNum).set(z,newCentroids.get(selectedCentroidNum).get(z) + r.get(z));
                        recordsInCentroid.set(selectedCentroidNum, recordsInCentroid.get(selectedCentroidNum) + 1);
                    }
                }

                for (int i = 0; i < centroids.size(); i++){

                    for (int j = 0; j < centroids.get(0).size() - 1; j++){
                        newCentroids.get(i).set(j, newCentroids.get(i).get(j)/recordsInCentroid.get(i));
                    }

                    if(distance(newCentroids.get(i), centroids.get(i)) > THRESHOLD){
                        runIteration = true;
                    }
                }

                centroids = newCentroids;
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

            selectRandomCentroids(Integer.parseInt(value.toString()));
            kMeans();

            int i = 0;
            for (ArrayList<Double> c: centroids) {
                StringBuilder out_record = new StringBuilder();
                for (Double d: c) {
                    for (int j = 0; j < c.size() - 1; j++){
                         out_record.append(c.get(j) + ";");
                    }
                    out_record.append(c.get(c.size() - 1));
                }
                context.write(null, new Text(Integer.toString(i) + ":" + out_record.toString()));
                i++;
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
        job.addCacheFile(new URI(args[0]+"/normalized_data.csv"));
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
