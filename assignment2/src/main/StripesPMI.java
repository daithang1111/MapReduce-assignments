/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import cern.colt.Arrays;
import edu.umd.cloud9.io.map.String2IntOpenHashMapWritable;
import edu.umd.cloud9.io.pair.PairOfStrings;

/**
 * <p>
 * Implementation of the "pairs" algorithm for computing co-occurrence matrices from a large text
 * collection. This algorithm is described in Chapter 3 of "Data-Intensive Text Processing with
 * MapReduce" by Lin &amp; Dyer, as well as the following paper:
 * </p>
 * 
 * <blockquote>Jimmy Lin. <b>Scalable Language Processing Algorithms for the Masses: A Case Study in
 * Computing Word Co-occurrence Matrices with MapReduce.</b> <i>Proceedings of the 2008 Conference
 * on Empirical Methods in Natural Language Processing (EMNLP 2008)</i>, pages 419-428.</blockquote>
 * 
 * @author Jimmy Lin
 */
public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  private static class MyMapper_first extends
      Mapper<LongWritable, Text, Text, String2IntOpenHashMapWritable> {
    private static final String2IntOpenHashMapWritable MAP = new String2IntOpenHashMapWritable();
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String line = value.toString();
      String cur = null;
      StringTokenizer itr = new StringTokenizer(line);
      ArrayList<String> seenTokens = new ArrayList<String>();
      while (itr.hasMoreTokens()) {
        cur = itr.nextToken();
        if (!seenTokens.contains(cur)) {
          seenTokens.add(cur);
        }
      }

      // only process >1 unique words
      // if (seenTokens.size() > 0) {
      for (int i = 0; i < seenTokens.size(); i++) {
        cur = seenTokens.get(i);
        MAP.clear();
        MAP.put("*", 1);
        for (int j = 0; j < seenTokens.size(); j++) {
          if (j != i) {
            MAP.put(seenTokens.get(j), 1);
          }
        }
        KEY.set(cur);
        context.write(KEY, MAP);
      }
      // }

    }
  }

  // ///////////////////MY SECOND MAPPER/////////////////////////////////
  // a simple mapper just emit every lines
  private static class MyMapper_second extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private static final DoubleWritable COUNT = new DoubleWritable();
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String line = value.toString();
      String term_count[] = line.split("\\t");
      if (term_count.length == 2) {
        COUNT.set(Double.parseDouble(term_count[1]));
        KEY.set(term_count[0]);
        context.write(KEY, COUNT);
      }

    }
  }

  // /////////////////END//////////////////////////////////////////////////

  // combiner
  private static class MyCombiner extends
      Reducer<Text, String2IntOpenHashMapWritable, Text, String2IntOpenHashMapWritable> {

    @Override
    public void reduce(Text key, Iterable<String2IntOpenHashMapWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<String2IntOpenHashMapWritable> iter = values.iterator();
      String2IntOpenHashMapWritable map = new String2IntOpenHashMapWritable();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      context.write(key, map);
    }
  }

  // reducer
  private static class MyReducer_first extends
      Reducer<Text, String2IntOpenHashMapWritable, PairOfStrings, DoubleWritable> {
    private static final DoubleWritable VALUE = new DoubleWritable();
    private static final PairOfStrings TWOWORDS = new PairOfStrings();
    private static final double docLog = Math.log(156215);// log of number of docs
    private double marginal = 0.0;

    @Override
    public void reduce(Text key, Iterable<String2IntOpenHashMapWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<String2IntOpenHashMapWritable> iter = values.iterator();
      String2IntOpenHashMapWritable map = new String2IntOpenHashMapWritable();

      while (iter.hasNext()) {
        map.plus(iter.next());
      }

      marginal = (double) (map.getInt("*"));

      //
      Set<String> set = map.keySet();
      for (String s : set) {
        if (!s.equals("*")) {
          if (map.getInt(s) >= 10) {
            // write in order A,B
            TWOWORDS.set(key.toString(), s);
            VALUE.set(-Math.log(marginal));
            context.write(TWOWORDS, VALUE);
            // write in reverse order
            TWOWORDS.set(s, key.toString());
            VALUE.set(docLog + Math.log((double) (map.getInt(s))) - Math.log(marginal));
            context.write(TWOWORDS, VALUE);
          }
        }
      }

    }
  }

  // //////////////////////////MY SECOND REDUCER//////////////////////////////
  // just simply aggregate
  private static class MyReducer_second extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private static final DoubleWritable VALUE = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<DoubleWritable> iter = values.iterator();
      double tempD = 0.0;

      while (iter.hasNext()) {
        tempD += iter.next().get();
      }
      VALUE.set(tempD);
      context.write(key, VALUE);
    }
  }

  // ///////////////////////////END/////////////////////////////////

  /**
   * Creates an instance of this tool.
   */
  public StripesPMI() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path")
        .create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path")
        .create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT) + "_TMP";// cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Job job_first = Job.getInstance(getConf());
    job_first.setJobName(StripesPMI.class.getSimpleName());
    job_first.setJarByClass(StripesPMI.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    job_first.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job_first, new Path(inputPath));
    FileOutputFormat.setOutputPath(job_first, new Path(outputPath));

    job_first.setMapOutputKeyClass(Text.class);
    job_first.setMapOutputValueClass(String2IntOpenHashMapWritable.class);
    job_first.setOutputKeyClass(PairOfStrings.class);// Text.class);// PairOfStrings.class);
    job_first.setOutputValueClass(DoubleWritable.class);
    job_first.setOutputFormatClass(TextOutputFormat.class);// changed

    job_first.setMapperClass(MyMapper_first.class);
    job_first.setCombinerClass(MyCombiner.class);
    job_first.setReducerClass(MyReducer_first.class);

    long startTime = System.currentTimeMillis();
    job_first.waitForCompletion(true);

    // ////////////////START.: run the second MR job to just aggregate result////////////////
    inputPath = outputPath;// cmdline.getOptionValue(INPUT);
    outputPath = cmdline.getOptionValue(OUTPUT);

    Job job_second = Job.getInstance(getConf());
    job_second.setJobName(StripesPMI.class.getSimpleName());
    job_second.setJarByClass(StripesPMI.class);

    // Delete the output directory if it exists already.
    outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    job_second.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job_second, new Path(inputPath));
    FileOutputFormat.setOutputPath(job_second, new Path(outputPath));

    job_second.setMapOutputKeyClass(Text.class);
    job_second.setMapOutputValueClass(DoubleWritable.class);
    job_second.setOutputKeyClass(Text.class);// PairOfStrings.class);
    job_second.setOutputValueClass(DoubleWritable.class);
    // job_second.setOutputFormatClass(TextOutputFormat.class);// changed

    job_second.setMapperClass(MyMapper_second.class);
    // job_second.setCombinerClass(MyCombiner.class);
    job_second.setReducerClass(MyReducer_second.class);

    job_second.waitForCompletion(true);

    // END////////////

    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}
