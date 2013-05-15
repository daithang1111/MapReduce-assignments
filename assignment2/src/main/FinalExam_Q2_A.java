import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

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
import org.apache.hadoop.io.IntWritable;
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
import edu.umd.cloud9.io.pair.PairOfInts;

public class FinalExam_Q2_A extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(FinalExam_Q2_A.class);

  private static class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
    private static final PairOfInts VALUE = new PairOfInts();
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String line = value.toString();
      String term_count[] = line.split("\\t");
      if (term_count.length == 2) {
        VALUE.set(Integer.parseInt(term_count[1]), 1);
        KEY.set(term_count[0]);
        context.write(KEY, VALUE);
      }
    }
  }

  private static class MyCombiner extends Reducer<Text, PairOfInts, Text, PairOfInts> {
    private static final PairOfInts VALUE = new PairOfInts();

    @Override
    public void reduce(Text key, Iterable<PairOfInts> values, Context context) throws IOException,
        InterruptedException {
      Iterator<PairOfInts> iter = values.iterator();
      HashMap<Integer, Integer> hash = new HashMap<Integer, Integer>();
      int left, right;
      while (iter.hasNext()) {
        PairOfInts pair = iter.next();
        left = pair.getLeftElement();
        right = pair.getRightElement();
        if (!hash.containsKey(left)) {
          hash.put(left, right);
        } else {
          hash.put(left, hash.get(left) + right);
        }
      }

      for (int i : hash.keySet()) {
        VALUE.set(i, hash.get(i));
        context.write(key, VALUE);
      }

    }
  }

  private static class MyReducer extends Reducer<Text, PairOfInts, Text, IntWritable> {
    private static final IntWritable VALUE = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<PairOfInts> values, Context context) throws IOException,
        InterruptedException {
      Iterator<PairOfInts> iter = values.iterator();
      int max = Integer.MIN_VALUE;
      int value = 0;
      //we assume that combiner is used to the maximum possible, so no duplicate for integers in the reducers
      //the following code is simple and working
      //if this assumption is not possible, we wil have to do like FinalExam_Q2_B reduce function.
      while (iter.hasNext()) {
        PairOfInts pair = iter.next();
        int tmp = pair.getRightElement();
        if (tmp > max) {
          max = tmp;
          value = pair.getLeftElement();
        }
      }
      VALUE.set(value);
      context.write(key, VALUE);
    }
  }

  private FinalExam_Q2_A() {
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
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + FinalExam_Q2_A.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(FinalExam_Q2_A.class.getSimpleName());
    job.setJarByClass(FinalExam_Q2_A.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(PairOfInts.class);
    job.setOutputKeyClass(Text.class);// Text.class);// PairOfStrings.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);// changed
    // this

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);

    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new FinalExam_Q2_A(), args);
  }
}
