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
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashMap;
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
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.example.pagerank.RunPageRankSchimmy;
import edu.umd.cloud9.io.array.ArrayListOfFloatsWritable;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of PageRank.
 * </p>
 * 
 * <p>
 * The starting and ending iterations will correspond to paths <code>/base/path/iterXXXX</code> and
 * <code>/base/path/iterYYYY</code>. As a example, if you specify 0 and 10 as the starting and
 * ending iterations, the driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 * 
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  /**
   * 
   * @author nguyentd4
   * 
   */
  // Mapper, no in-mapper combining.
  private static class MapClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final PageRankNode intermediateMass = new PageRankNode();

    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();
    private static int sourcesize = 0;

    @Override
    public void setup(Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
      // I think hadoop already configs this to be comma separated
      String inputSources[] = context.getConfiguration().getStrings("MultiSources");
      sourcesize = inputSources.length;
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacenyList().size() > 0) {
        // afw.clear();
        ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();
        // ArrayListOfFloatsWritable tmpAFW = new ArrayListOfFloatsWritable();
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacenyList();
        // float[] mass = new float[sourcesize];
        // float[] currentPRs = node.getPageRank().getArray();
        context.getCounter(PageRank.edges).increment(list.size());
        for (int t = 0; t < sourcesize; t++) {
          mass.add(node.getPageRank().get(t) - (float) StrictMath.log(list.size()));
          // tmpAFW.add(Float.NEGATIVE_INFINITY);// init this tmp
        }

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i));
          intermediateMass.setType(PageRankNode.Type.Mass);
          intermediateMass.setPageRank(mass);

          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }

  // Mapper with in-mapper combiner optimization.
  private static class MapWithInMapperCombiningClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // For buffering PageRank mass contributes keyed by destination node.
    // private static HMapIF[] map=null;
    private static final HashMap<Integer, ArrayListOfFloatsWritable> map = new HashMap<Integer, ArrayListOfFloatsWritable>();
    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();
    // private static final ArrayListWritable<IntWritable> multiSources = new
    // ArrayListWritable<IntWritable>();
    private static int sourcesize = 0;

    // private static final ArrayListOfFloatsWritable afw = new ArrayListOfFloatsWritable();

    // private static float[] afw = null;

    @Override
    public void setup(Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
      // I think hadoop already configs this to be comma separated
      String inputSources[] = context.getConfiguration().getStrings("MultiSources");
      sourcesize = inputSources.length;

      // for (int i = 0; i < inputSources.length; i++) {
      // multiSources.add(new IntWritable(Integer.parseInt(inputSources[i])));
      // }
      // afw = new float[sourcesize];
      // //init map
      // map =new HMapIF[sourcesize];
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;
      int massMessagesSaved = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacenyList().size() > 0) {
        // afw.clear();
        ArrayListOfFloatsWritable afw = new ArrayListOfFloatsWritable();
        ArrayListOfFloatsWritable tmpAFW = new ArrayListOfFloatsWritable();
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacenyList();
        // float[] mass = new float[sourcesize];
        // float[] currentPRs = node.getPageRank().getArray();
        context.getCounter(PageRank.edges).increment(list.size());
        for (int t = 0; t < sourcesize; t++) {
          afw.add(node.getPageRank().get(t) - (float) StrictMath.log(list.size()));
          tmpAFW.add(Float.NEGATIVE_INFINITY);// init this tmp
        }

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          int neighbor = list.get(i);

          if (map.containsKey(neighbor)) {
            // Already message destined for that node; add PageRank mass contribution.
            massMessagesSaved++;

            for (int t = 0; t < sourcesize; t++) {
              tmpAFW.set(t, sumLogProbs(map.get(neighbor).get(t), afw.get(t)));
            }

            map.put(neighbor, tmpAFW);
          } else {
            // New destination node; add new entry in map.
            massMessages++;
            map.put(neighbor, afw);
          }
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
      context.getCounter(PageRank.massMessagesSaved).increment(massMessagesSaved);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      // Now emit the messages all at once.
      IntWritable k = new IntWritable();
      PageRankNode mass = new PageRankNode();

      for (int e : map.keySet()) {
        k.set(e);

        mass.setNodeId(e);
        mass.setType(PageRankNode.Type.Mass);
        mass.setPageRank(map.get(e));

        context.write(k, mass);
      }
    }
  }

  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private static final PageRankNode intermediateMass = new PageRankNode();
    private static int sourcesize = 0;

    @Override
    public void setup(Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
      // I think hadoop already configs this to be comma separated
      String inputSources[] = context.getConfiguration().getStrings("MultiSources");
      sourcesize = inputSources.length;

    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context)
        throws IOException, InterruptedException {
      int massMessages = 0;

      ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();// Float.NEGATIVE_INFINITY;
      for (int t = 0; t < sourcesize; t++) {
        mass.add(Float.NEGATIVE_INFINITY);
      }

      // Remember, PageRank mass is stored as a log prob.
      for (PageRankNode n : values) {

        if (n.getType() == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
          // Accumulate PageRank mass contributions.
          for (int t = 0; t < sourcesize; t++) {
            mass.set(t, sumLogProbs(mass.get(t), n.getPageRank().get(t)));
          }

          massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(PageRankNode.Type.Mass);
        intermediateMass.setPageRank(mass);

        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.
    private ArrayListOfFloatsWritable totalMass = new ArrayListOfFloatsWritable();
    private static int sourcesize = 0;

    @Override
    public void setup(Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode>.Context context) {
      // I think hadoop already configs this to be comma separated
      String inputSources[] = context.getConfiguration().getStrings("MultiSources");
      sourcesize = inputSources.length;
      for (int t = 0; t < sourcesize; t++) {
        totalMass.add(Float.NEGATIVE_INFINITY);
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<PageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      PageRankNode node = new PageRankNode();

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());

      int massMessagesReceived = 0;
      int structureReceived = 0;

      ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();// Float.NEGATIVE_INFINITY;
      for (int t = 0; t < sourcesize; t++) {
        mass.add(Float.NEGATIVE_INFINITY);
      }
      while (values.hasNext()) {
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacenyList();
          structureReceived++;

          node.setAdjacencyList(list);
        } else {
          // This is a message that contains PageRank mass; accumulate.

          for (int t = 0; t < sourcesize; t++) {
            mass.set(t, sumLogProbs(mass.get(t), n.getPageRank().get(t)));
          }

          massMessagesReceived++;
        }
      }

      // Update the final accumulated PageRank mass.
      node.setPageRank(mass);
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);

        // Keep track of total PageRank mass.
        // totalMass = sumLogProbs(totalMass, mass);
        for (int t = 0; t < sourcesize; t++) {
          totalMass.set(t, sumLogProbs(totalMass.get(t), mass.get(t)));
        }
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this reducer.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      for (int t = 0; t < sourcesize; t++) {// make sure if this works
        out.writeFloat(totalMass.get(t));
      }
      out.close();
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private static class MapPageRankMassDistributionClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private ArrayListOfFloatsWritable missingMass = new ArrayListOfFloatsWritable();
    private int sourcesize = 0;
    // private int singleSrc = -1;
    // private static final ArrayListWritable<IntWritable> multiSources = new
    // ArrayListWritable<IntWritable>();
    private static int[] multiSources = null;
    private int nodeCnt = 0;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      // singleSrc = conf.getInt("SingleSource", 0);
      String inputSources[] = context.getConfiguration().getStrings("MultiSources");
      String mMasses[] = context.getConfiguration().getStrings("MissingMass");// this is missing
                                                                              // masses
      sourcesize = inputSources.length;
      multiSources = new int[sourcesize];
      for (int t = 0; t < sourcesize; t++) {
        missingMass.add(Float.parseFloat(mMasses[t]));// /init
        // multiSources.add(new IntWritable(Integer.parseInt(inputSources[t])));
        multiSources[t] = Integer.parseInt(inputSources[t]);
      }
      // missingMass = conf.getFloat("MissingMass", 0.0f);
      nodeCnt = conf.getInt("NodeCount", 0);
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException,
        InterruptedException {
      ArrayListOfFloatsWritable p = node.getPageRank();

      // if single source, add all mass
      for (int t = 0; t < sourcesize; t++) {
        if (nid.get() == multiSources[t]) {
          float jump = (float) (Math.log(ALPHA));
          float link = (float) Math.log(1.0f - ALPHA)
              + sumLogProbs(p.get(t), (float) (Math.log(missingMass.get(t))));

          p.set(t, sumLogProbs(jump, link));
        } else {
          p.set(t, p.get(t) + (float) Math.log(1.0f - ALPHA));
        }
      }

      node.setPageRank(p);

      context.write(nid, node);
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
  }

  public RunPersonalizedPageRankBasic() {
  }

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  //
  private static final String SOURCES = "sources";

  // private static final String COMBINER = "useCombiner";
  // private static final String INMAPPER_COMBINER = "useInMapperCombiner";
  // private static final String RANGE = "range";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    // options.addOption(new Option(COMBINER, "use combiner"));
    // options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
    // options.addOption(new Option(RANGE, "use range partitioner"));

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("base path")
        .create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("start iteration")
        .create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("end iteration")
        .create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("number of nodes")
        .create(NUM_NODES));
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

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) || !cmdline.hasOption(END)
        || !cmdline.hasOption(NUM_NODES) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
    String sources = cmdline.getOptionValue(SOURCES);
    // boolean useCombiner = cmdline.hasOption(COMBINER);
    // boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
    // boolean useRange = cmdline.hasOption(RANGE);

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    LOG.info(" - sources: " + sources);
    // LOG.info(" - use combiner: " + useCombiner);
    // LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
    // LOG.info(" - user range partitioner: " + useRange);

    // get the first source
    // int singleSource = Integer.parseInt(sources.split(",")[0]);// fisrt source

    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, sources);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes, String sources)
      throws Exception {
    // Each iteration consists of two phases (two MapReduce jobs).
    String[] separateSources = sources.split("\\,");
    int sourcesize = separateSources.length;
    ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();
    ArrayListOfFloatsWritable missing = new ArrayListOfFloatsWritable();
    // Job 1: distribute PageRank mass along outgoing edges.
    mass = phase1(i, j, basePath, numNodes, sourcesize, sources);
    // float mass = phase1(i, j, basePath, numNodes);

    // Find out how much PageRank mass got lost at the dangling nodes.
    for (int t = 0; t < sourcesize; t++) {
      missing.add(1.0f - (float) StrictMath.exp(mass.get(t)));
    }

    // it seems that we must use string to represent missing masses
    if (sourcesize == 0)
      return;// this should never happens

    StringBuffer missingMasses = new StringBuffer();
    missingMasses.append(missing.get(0));
    for (int t = 1; t < sourcesize; t++) {
      missingMasses.append("," + missing.get(t));
    }

    // Job 2: distribute missing mass, take care of random jump factor.
    phase2(i, j, missingMasses.toString(), basePath, numNodes, sources);
  }

  /**
   * 
   * @param i
   * @param j
   * @param basePath
   * @param numNodes
   * @param ssize
   * @return
   * @throws Exception
   */
  private ArrayListOfFloatsWritable phase1(int i, int j, String basePath, int numNodes, int ssize,
      String sources) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    // LOG.info(" - useCombiner: " + useCombiner);
    // LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
    LOG.info("computed number of partitions: " + numPartitions);

    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    // set multi sources
    job.getConfiguration().setStrings("MultiSources", sources);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    // job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    // job.setMapperClass(MapWithInMapperCombiningClass.class);
    job.setMapperClass(MapClass.class);
    // if (useCombiner) {
    job.setCombinerClass(CombineClass.class);
    // }

    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");

    // initiate masses
    ArrayListOfFloatsWritable mass = new ArrayListOfFloatsWritable();
    for (int t = 0; t < ssize; t++) {
      mass.add(Float.NEGATIVE_INFINITY);
    }
    //
    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());
      for (int t = 0; t < ssize; t++) {
        mass.set(t, sumLogProbs(mass.get(t), fin.readFloat()));
      }
      fin.close();
    }

    return mass;
  }

  /**
   * 
   * @param i
   * @param j
   * @param missingMasses
   * @param basePath
   * @param numNodes
   * @param sources
   * @throws Exception
   */
  private void phase2(int i, int j, String missingMasses, String basePath, int numNodes,
      String sources) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    LOG.info("missing PageRank mass: " + missingMasses);
    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().setStrings("MissingMass", missingMasses);// /////set as string
    // set multi sources
    job.getConfiguration().setStrings("MultiSources", sources);

    job.getConfiguration().setInt("NodeCount", numNodes);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0
        + " seconds");
  }

  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
}
