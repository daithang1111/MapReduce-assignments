import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

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

public class FinalExam_Q2_B extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(FinalExam_Q2_B.class);

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
      Iterator<PairOfInts> iter = values.iterator();// this values should be sorted, ordered by left
                                                    // elements
      TreeMap<Integer, Integer> hash = new TreeMap<Integer, Integer>();
      int left = 0, right = 0, totalCount = 0;
      while (iter.hasNext()) {
        PairOfInts pair = iter.next();
        left = pair.getLeftElement();
        right = pair.getRightElement();
        totalCount += right;
        if (!hash.containsKey(left)) {
          hash.put(left, right);
        } else {
          hash.put(left, hash.get(left) + right);
        }
      }

      if (hash.size() == 1) {
        VALUE.set(hash.firstKey());
      } else {

        int middleMan = (totalCount / 2) + 1;// this is the middle man

        // since
        int prevValue = Integer.MIN_VALUE, currentValue = Integer.MIN_VALUE;
        int sumOfCount = 0;// stop at index where sumOfCount =totalCount/2;

        for (int i : hash.keySet()) {
          prevValue = currentValue;
          currentValue = i;
          sumOfCount += hash.get(i);

          if (sumOfCount >= middleMan) {
            if (totalCount % 2 == 1) {
              VALUE.set(currentValue);
            } else {
              if (sumOfCount * 2 < middleMan * 2 + hash.get(i) || prevValue == Integer.MIN_VALUE) {
                VALUE.set(currentValue);
              } else {
                VALUE.set(((prevValue + currentValue) / 2));
              }
            }
            break;
          }
        }
      }
      context.write(key, VALUE);
    }
  }

  private FinalExam_Q2_B() {
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

    LOG.info("Tool name: " + FinalExam_Q2_B.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(FinalExam_Q2_B.class.getSimpleName());
    job.setJarByClass(FinalExam_Q2_B.class);

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
    // job.setPartitionerClass(MyPartitioner.class);
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
    ToolRunner.run(new FinalExam_Q2_B(), args);
  }
}
