import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class PairsPMI_nocombiner extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI_nocombiner.class);

  protected static class MyMapper_first extends
      Mapper<LongWritable, Text, PairOfStrings, DoubleWritable> {
    private static final DoubleWritable ONE = new DoubleWritable(1);
    private static final PairOfStrings BIGRAM = new PairOfStrings();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException,
        InterruptedException {
      String line = value.toString();

      String prev = null;
      StringTokenizer itr = new StringTokenizer(line);
      ArrayList<String> seenTokens = new ArrayList<String>();
      while (itr.hasMoreTokens()) {
        String cur = itr.nextToken();
        if (seenTokens.size() == 0) {
          seenTokens.add(cur);
        } else {
          if (seenTokens.size() > 0 && !seenTokens.contains(cur)) {
            for (int i = 0; i < seenTokens.size(); i++) {
              prev = seenTokens.get(i);
              BIGRAM.set(prev, cur);
              context.write(BIGRAM, ONE);
              BIGRAM.set(cur, prev);
              context.write(BIGRAM, ONE);
            }
            seenTokens.add(cur);
          }
        }
      }
      for (int i = 0; i < seenTokens.size(); i++) {
        BIGRAM.set(seenTokens.get(i), "%");
        context.write(BIGRAM, ONE);
      }
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
  protected static class MyCombiner extends
      Reducer<PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> {
    private static final DoubleWritable SUM = new DoubleWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      double sum = 0.0;
      Iterator<DoubleWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  // reducer
  protected static class MyReducer_first extends
      Reducer<PairOfStrings, DoubleWritable, PairOfStrings, DoubleWritable> {
    private static final DoubleWritable VALUE = new DoubleWritable();
    private static final PairOfStrings TWOWORDS = new PairOfStrings();
    private static final double docLog = Math.log(156215);// log of number of docs
    private double marginal = 0.0;

    @Override
    public void reduce(PairOfStrings key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {
      double sum = 0.0d;
      Iterator<DoubleWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }

      if (key.getRightElement().equals("%")) {
        marginal = sum;
      } else {
        if (sum >= 10) {
          // write in correct order
          // A, B -log(count(A*))
          VALUE.set(-Math.log(marginal));
          context.write(key, VALUE);

          // write in reverse order B,A
          // B, A docLog + log(A,B)-log(A*)
          VALUE.set(docLog + Math.log(sum) - Math.log(marginal));
          TWOWORDS.set(key.getRightElement(), key.getLeftElement());
          context.write(TWOWORDS, VALUE);

          // by doing this, in the output file we have 2 rows for A,B which sum up to PMI of A,B
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
  // partitioner
  protected static class MyPartitioner extends Partitioner<PairOfStrings, DoubleWritable> {
    @Override
    public int getPartition(PairOfStrings key, DoubleWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  private PairsPMI_nocombiner() {
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

    LOG.info("Tool name: " + PairsPMI_nocombiner.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(PairsPMI_nocombiner.class.getSimpleName());
    job.setJarByClass(PairsPMI_nocombiner.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(PairOfStrings.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(PairOfStrings.class);// Text.class);// PairOfStrings.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);// changed
    // this

    job.setMapperClass(MyMapper_first.class);
//    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(MyReducer_first.class);
    job.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);

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
    ToolRunner.run(new PairsPMI_nocombiner(), args);
  }
}
