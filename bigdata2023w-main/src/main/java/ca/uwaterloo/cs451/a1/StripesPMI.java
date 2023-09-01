/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a1;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.map.HMapStFW;
import tl.lin.data.pair.PairOfFloatInt;
import tl.lin.data.map.HashMapWritable;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

public class StripesPMI  extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  // ---------------------------------- First Job ---------------------------------------
  private static final class MyFirstMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private static final FloatWritable ONE = new FloatWritable(1);
    private static final Text WORD = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      List<String> tokens = Tokenizer.tokenize(value.toString());

      int MAX_LEN = 40;
      int listLen = Math.min(tokens.size(), MAX_LEN);
      
      // Count for unique word in a single line
      List<String> realTokens = new ArrayList<>();
      for (int i = 0; i < listLen; i++) {
        String currToken = tokens.get(i);
        if (!realTokens.contains(currToken)) {
          realTokens.add(currToken);
          WORD.set(currToken);
          context.write(WORD, ONE);
        }
      }

      // Count for total line
      WORD.set("*");
      context.write(WORD, ONE);

    }
  }

  private static final class MyFirstCombiner extends
      Reducer<Text, FloatWritable, Text, FloatWritable> {
    private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      Iterator<FloatWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static final class MyFirstReducer extends
      Reducer<Text, FloatWritable, Text, FloatWritable> {
        private static final FloatWritable SUM = new FloatWritable();

    @Override
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
          int sum = 0;
          Iterator<FloatWritable> iter = values.iterator();
          while (iter.hasNext()) {
            sum += iter.next().get();
          }
          SUM.set(sum);
          context.write(key, SUM);
    }
  }

  // ----------------------------- Main Job ---------------------------------

  protected static final class MyMapper extends Mapper<LongWritable, Text, Text, HMapStFW> {
    private static final Text TEXT = new Text();
    private static final HMapStFW  Dic = new HMapStFW();

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      
      int MAX_LEN = 40;
      List<String> tokens = Tokenizer.tokenize(value.toString());
      List<String> realTokens = new ArrayList<>();

      if (tokens.size() < 2) return;

      int listLen = Math.min(tokens.size(), MAX_LEN);
      for (int i = 0; i < listLen; i++) {
        String currToken = tokens.get(i);
        if (!realTokens.contains(currToken)) {
          realTokens.add(currToken);
        }
      }

      int realTokenLen = realTokens.size();
      if (realTokenLen < 2) return;

      for (int i = 0; i < realTokenLen; i++) {
        Dic.clear();
        for (int j = 0; j < realTokenLen; j++) {
          if (i != j) {
            Dic.put(realTokens.get(j), 1);
          }
        }

        TEXT.set(realTokens.get(i));
        context.write(TEXT, Dic);
      }
      
    }
  }

  private static final class MyCombiner extends Reducer<Text, HMapStFW, Text, HMapStFW> {
    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  private static final class MyReducer extends Reducer<Text, HMapStFW, Text, HashMapWritable<Text, PairOfFloatInt>> {
    private static HashMap<String, Integer> wordOccurMap = new HashMap<>();
    private static final Text KEY = new Text();
    private static final HashMapWritable<Text, PairOfFloatInt> MAP = new HashMapWritable<>();

    @Override
        public void setup(Context context) throws IOException {
            Configuration config = context.getConfiguration();
            FileSystem fs = FileSystem.get(config);

            String tempFilePath = config.get("tempFilePath");
            Path inFile = new Path(tempFilePath + "/part-r-00000");

            if (!fs.exists(inFile)) {
                throw new IOException("Failed to find file: " + inFile.toString());
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inFile)));

            String line;
            line = br.readLine();
            while (line != null){
                // Read side data from the tempfile
                String[] lineTokens = new String[2];
                lineTokens = line.split("\\s+");
                
                // Store the side data into a HashMap
                String word = lineTokens[0] ;
                int occurrence = (int) Float.parseFloat(lineTokens[1]) ;
                wordOccurMap.put(word, occurrence) ;

                line = br.readLine();
            }

            br.close() ;

        }

    @Override
    public void reduce(Text key, Iterable<HMapStFW> values, Context context)
        throws IOException, InterruptedException {
      Iterator<HMapStFW> iter = values.iterator();
      HMapStFW map = new HMapStFW();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      Configuration config = context.getConfiguration();
      float numThreshold = Float.parseFloat(config.get("threshold"));
      MAP.clear();

      String keyWord = key.toString(); 
      float sum = 0.0f;
      for (String entry : map.keySet()) {
        String cooccurWord = entry;
        sum = map.get(entry);
        if (sum >= numThreshold) {
          float totalLine = wordOccurMap.get("*");
          String pairX = keyWord;
          String pairY = cooccurWord;
          float probX = wordOccurMap.get(pairX) / totalLine;
          float probY = wordOccurMap.get(pairY) / totalLine;
          float probPairXY = sum / totalLine;
          float PMI =  (float) (Math.log10( probPairXY / (probX * probY)));

          PairOfFloatInt PMI_OUTPUT = new PairOfFloatInt();
          PMI_OUTPUT.set(PMI, (int) sum);
          MAP.put(new Text(cooccurWord), PMI_OUTPUT);
        }
      }
      
      if (MAP.size() > 0) {
        KEY.set(key);
        context.write(KEY, MAP);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  private StripesPMI() {}

  private static final class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    String output;

    @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
    int numReducers = 1;

    @Option(name = "-textOutput", usage = "use TextOutputFormat (otherwise, SequenceFileOutputFormat)")
    boolean textOutput = false;

    @Option(name = "-threshold", required = false, usage = "the threshold of co-occurrence")
    int numThreshold = 1;
  }

  /**
   * Runs this tool.
   */
  @Override
  public int run(String[] argv) throws Exception {
    final Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool name: " + StripesPMI.class.getSimpleName());
    // ------------------------ Start the first job -------------------------

    String tempFilePath = args.output + "-tempfile";

    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + tempFilePath);
    LOG.info(" - num reducers: " + args.numReducers);
    LOG.info(" - text output: " + args.textOutput);
    LOG.info(" - num threshold: " + args.numThreshold);

    Configuration conf = getConf();
    conf.set("tempFilePath", tempFilePath);
    conf.set("threshold", Integer.toString(args.numThreshold));

    Job firstJob = Job.getInstance(getConf());
    firstJob.setJobName(PairsPMI.class.getSimpleName());
    firstJob.setJarByClass(PairsPMI.class);

    // firstJob.setNumReduceTasks(args.numReducers);
    firstJob.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(firstJob, new Path(args.input));
    FileOutputFormat.setOutputPath(firstJob, new Path(tempFilePath));

    firstJob.setMapOutputKeyClass(Text.class);
    firstJob.setMapOutputValueClass(FloatWritable.class);
    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(FloatWritable.class);
    firstJob.setOutputFormatClass(TextOutputFormat.class);

    firstJob.setMapperClass(MyFirstMapper.class);
    firstJob.setCombinerClass(MyFirstCombiner.class);
    firstJob.setReducerClass(MyFirstReducer.class);

    firstJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    firstJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    firstJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    firstJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    firstJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    Path outputDir = new Path(tempFilePath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    firstJob.waitForCompletion(true);
    System.out.println("First Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    
    // --------------- Start the main job -------------------

    Job job = Job.getInstance(getConf());
    job.setJobName(StripesPMI.class.getSimpleName());
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(args.numReducers);

    FileInputFormat.setInputPaths(job, new Path(args.input));
    FileOutputFormat.setOutputPath(job, new Path(args.output));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HMapStFW.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HashMapWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);

    job.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
    job.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    job.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    job.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // Delete the output directory if it exists already.
    outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Main Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   *
   * @param args command-line arguments
   * @throws Exception if tool encounters an exception
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
