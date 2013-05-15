import java.io.IOException;
import java.util.Iterator;
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

public class FinalExam_Q1_A extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(FinalExam_Q1_A.class);

  protected static class MyMapper_first extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String line = value.toString();

      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        String cur = itr.nextToken();
        KEY.set(cur);
        context.write(KEY, ONE);
      }
    }
  }

  // reducer
  protected static class MyReducer_first extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable VALUE = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      VALUE.set(-sum);
      context.write(key, VALUE);
    }
  }

  // ///////////////////MY SECOND MAPPER/////////////////////////////////
  // a simple mapper just emit every lines
  private static class MyMapper_second extends Mapper<LongWritable, Text, IntWritable, Text> {
    private static final IntWritable KEY = new IntWritable();
    private static final Text VALUE = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String line = value.toString();
      String term_count[] = line.split("\\t");
      if (term_count.length == 2) {
        KEY.set(Integer.parseInt(term_count[1]));
        VALUE.set(term_count[0]);
        context.write(KEY, VALUE);
      }

    }
  }

  // //////////////////////////MY SECOND REDUCER//////////////////////////////
  // just simply aggregate
  // only 1 reducer is used
  private static class MyReducer_second extends Reducer<IntWritable, Text, Text, IntWritable> {
    private static final IntWritable VALUE = new IntWritable();
    private static final Text KEY = new Text();
    private int count = 0;

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
      Iterator<Text> iter = values.iterator();
      while (iter.hasNext()) {
        KEY.set(iter.next());
        VALUE.set(++count);
        context.write(KEY, VALUE);
      }
    }
  }

  // ///////////////////////////END/////////////////////////////////

  private FinalExam_Q1_A() {
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
    String outputPath = cmdline.getOptionValue(OUTPUT) + "_TMP";
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool name: " + FinalExam_Q1_A.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(FinalExam_Q1_A.class.getSimpleName());
    job.setJarByClass(FinalExam_Q1_A.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);// Text.class);// PairOfStrings.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);// changed
    // this

    job.setMapperClass(MyMapper_first.class);
    job.setReducerClass(MyReducer_first.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);

    // ////////////////START.: run the second MR job to just aggregate result////////////////
    inputPath = outputPath;// cmdline.getOptionValue(INPUT);
    outputPath = cmdline.getOptionValue(OUTPUT);

    Job job_second = Job.getInstance(getConf());
    job_second.setJobName(FinalExam_Q1_A.class.getSimpleName());
    job_second.setJarByClass(FinalExam_Q1_A.class);

    // Delete the output directory if it exists already.
    outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    job_second.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job_second, new Path(inputPath));
    FileOutputFormat.setOutputPath(job_second, new Path(outputPath));

    job_second.setMapOutputKeyClass(IntWritable.class);
    job_second.setMapOutputValueClass(Text.class);
    job_second.setOutputKeyClass(Text.class);// PairOfStrings.class);
    job_second.setOutputValueClass(IntWritable.class);
    job_second.setOutputFormatClass(TextOutputFormat.class);// changed

    job_second.setMapperClass(MyMapper_second.class);
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
    ToolRunner.run(new FinalExam_Q1_A(), args);
  }
}
