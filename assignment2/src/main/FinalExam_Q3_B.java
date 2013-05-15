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
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.pair.PairOfInts;

public class FinalExam_Q3_B extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(FinalExam_Q3_B.class);

  private static class MyMapper extends Mapper<LongWritable, Text, IntWritable, PairOfInts> {
    private static final PairOfInts VALUE = new PairOfInts();
    private static final IntWritable KEY = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String line = value.toString();
      String term_count[] = line.split("\\t");

      int origin = Integer.parseInt(term_count[0]);
      for (int i = 1; i < term_count.length; i++) {
        KEY.set(origin);
        VALUE.set(Integer.parseInt(term_count[i]), 1);
        context.write(KEY, VALUE);

        for (int j = i + 1; j < term_count.length; j++) {
          KEY.set(Integer.parseInt(term_count[i]));
          VALUE.set(Integer.parseInt(term_count[j]), 2);
          context.write(KEY, VALUE);

          KEY.set(Integer.parseInt(term_count[j]));
          VALUE.set(Integer.parseInt(term_count[i]), 2);
          context.write(KEY, VALUE);

        }
      }
    }
  }

  private static class MyReducer extends
      Reducer<IntWritable, PairOfInts, IntWritable, ArrayListOfIntsWritable> {
    private static final ArrayListOfIntsWritable VALUE = new ArrayListOfIntsWritable();

    @Override
    public void reduce(IntWritable key, Iterable<PairOfInts> values, Context context)
        throws IOException, InterruptedException {
      Iterator<PairOfInts> iter = values.iterator();

      HashMap<Integer, Boolean> oneHop = new HashMap<Integer, Boolean>();
      HashMap<Integer, Boolean> twoHop = new HashMap<Integer, Boolean>();

      while (iter.hasNext()) {
        PairOfInts pair = iter.next();
        if (pair.getRightElement() == 1) {
          oneHop.put(pair.getLeftElement(), true);
        } else {
          twoHop.put(pair.getLeftElement(), true);
        }
      }

      // clustering coefficient = num of twohop, onehop/one hop*(onehope+1)/2.

      int oneTwoHop = 0;
      for (int n : twoHop.keySet()) {
        if (oneHop.containsKey(n)) {// if we want to remove direct link, else remove this
          oneTwoHop++;
        }
      }
      oneTwoHop = oneTwoHop / 2;
      int totalConnections = oneHop.size() * (oneHop.size() + 1) / 2;
      VALUE.add(oneTwoHop);
      VALUE.add(totalConnections);

      context.write(key, VALUE);
      VALUE.clear();
    }
  }

  private FinalExam_Q3_B() {
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

    LOG.info("Tool name: " + FinalExam_Q3_B.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(FinalExam_Q3_B.class.getSimpleName());
    job.setJarByClass(FinalExam_Q3_B.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PairOfInts.class);
    job.setOutputKeyClass(IntWritable.class);// IntWritable.class);// PairOfStrings.class);
    job.setOutputValueClass(ArrayListOfIntsWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);// changed
    // this

    job.setMapperClass(MyMapper.class);
    // job.setCombinerClass(MyReducer.class);
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
    ToolRunner.run(new FinalExam_Q3_B(), args);
  }
}
