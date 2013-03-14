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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.TopNScoredObjects;
import edu.umd.cloud9.util.pair.PairOfObjectFloat;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

  private static class MyMapper extends
      Mapper<IntWritable, PageRankNode, PairOfInts, FloatWritable> {
    private TopNScoredObjects<Integer> queue[];
    // private static final ArrayListWritable<IntWritable> multiSources = new
    // ArrayListWritable<IntWritable>();
    private static int sourcesize = 0;
    private static final PairOfInts poi = new PairOfInts();

    @SuppressWarnings("unchecked")
    @Override
    public void setup(Context context) throws IOException {
      String inputSources[] = context.getConfiguration().getStrings("MultiSources");
      sourcesize = inputSources.length;
      int k = context.getConfiguration().getInt("n", 100);
      // queue[0] = new TopNScoredObjects<Integer>(k);
      queue = new TopNScoredObjects[sourcesize];
      for (int i = 0; i < sourcesize; i++) {
        // multiSources.add(new IntWritable(Integer.parseInt(inputSources[i])));
        queue[i] = new TopNScoredObjects<Integer>(k);
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      for (int t = 0; t < sourcesize; t++) {
        queue[t].add(node.getNodeId(), node.getPageRank().get(t));
      }

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      // IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for (int t = 0; t < sourcesize; t++) {
        for (PairOfObjectFloat<Integer> pair : queue[t].extractAll()) {
          // key.set(pair.getLeftElement());
          poi.set(pair.getLeftElement(), t);
          value.set(pair.getRightElement());
          context.write(poi, value);
        }
      }

    }
  }

  private static class MyReducer extends
      Reducer<PairOfInts, FloatWritable, IntWritable, FloatWritable> {
    private static TopNScoredObjects<Integer> queue[];
    // private static final ArrayListWritable<IntWritable> multiSources = new
    // ArrayListWritable<IntWritable>();
    private static int sourcesize = 0;

    // private static final PairOfWritables<IntWritable, IntWritable> pow = new
    // PairOfWritables<IntWritable, IntWritable>();

    @SuppressWarnings("unchecked")
    @Override
    public void setup(Context context) throws IOException {
      String inputSources[] = context.getConfiguration().getStrings("MultiSources");
      sourcesize = inputSources.length;
      int k = context.getConfiguration().getInt("n", 100);
      // queue[0] = new TopNScoredObjects<Integer>(k);
      queue = new TopNScoredObjects[sourcesize];
      for (int i = 0; i < sourcesize; i++) {
        // multiSources.add(new IntWritable(Integer.parseInt(inputSources[i])));
        queue[i] = new TopNScoredObjects<Integer>(k);
      }
    }

    @Override
    public void reduce(PairOfInts nid_source, Iterable<FloatWritable> iterable, Context context)
        throws IOException {
      Iterator<FloatWritable> iter = iterable.iterator();
      queue[nid_source.getRightElement()].add(nid_source.getLeftElement(), iter.next().get());

      // Shouldn't happen. Throw an exception.
      if (iter.hasNext()) {
        throw new RuntimeException();
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      IntWritable key = new IntWritable();
      FloatWritable value = new FloatWritable();

      for (int t = 0; t < sourcesize; t++) {
        for (PairOfObjectFloat<Integer> pair : queue[t].extractAll()) {
          key.set(pair.getLeftElement());
          value.set(pair.getRightElement());
          context.write(key, value);
        }
      }
    }
  }

  public ExtractTopPersonalizedPageRankNodes() {
  }

  private static final String INPUT = "input";
  // private static final String OUTPUT = "output";
  private static final String TOP = "top";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path")
        .create(INPUT));
    // options.addOption(OptionBuilder.withArgName("path").hasArg()
    // .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("top n")
        .create(TOP));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of sources").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(TOP) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    // String outputPath = cmdline.getOptionValue(OUTPUT);
    // create tmpOutputPath
    String outputPath = "tmpTopScore";//

    int n = Integer.parseInt(cmdline.getOptionValue(TOP));
    String sources = cmdline.getOptionValue(SOURCES);
    LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
    LOG.info(" - input: " + inputPath);
    // LOG.info(" - output: " + outputPath);
    LOG.info(" - top: " + n);
    LOG.info(" - sources: " + sources);
    // get the first source
    // int singleSource = Integer.parseInt(sources.split(",")[0]);// fisrt source

    Configuration conf = getConf();
    conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
    conf.setInt("n", n);

    Job job = Job.getInstance(conf);
    job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
    job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);
    job.getConfiguration().setStrings("MultiSources", sources);// pass sources to mapper/reducers
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(PairOfInts.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    FileSystem.get(conf).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    // read the "tmpTopScore" to printout result
    FileSystem fs = FileSystem.get(conf);

    FSDataInputStream fin = fs.open(new Path(outputPath + "/part-r-00000"));
    BufferedReader d = new BufferedReader(new InputStreamReader(fin));
    String strLine;
//    float pagerankScore = 0.0f;
    double pagerankScore = 0.0f;
    int nodeId = 0;
    String[] mSources = sources.split("\\,");
    // int sSize = mSources.length;
    int sCount = 0;
    // System.out.println("Source:\t"+singleSource);
    while ((strLine = d.readLine()) != null) {
      if (sCount % n == 0) {
        System.out.println("\n");
        System.out.println("Source:\t" + mSources[sCount / n]);
      }      
      String[] tmpStr = strLine.split("\\t");
      nodeId = Integer.parseInt(tmpStr[0]);
      pagerankScore = (float)StrictMath.exp(Double.parseDouble(tmpStr[1]));
      System.out.println(String.format("%.5f %d", pagerankScore, nodeId));
      sCount++;
    }

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
    System.exit(res);
  }
}
