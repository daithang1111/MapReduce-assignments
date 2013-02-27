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
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;

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
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.util.fd.Int2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Int2IntFrequencyDistributionEntry;

public class LookupPostingsCompressed extends Configured implements Tool {
  private static final String INDEX = "index";
  private static final String COLLECTION = "collection";

  private LookupPostingsCompressed() {
  }

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path")
        .create(INDEX));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path")
        .create(COLLECTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INDEX) || !cmdline.hasOption(COLLECTION)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(LookupPostingsCompressed.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String indexPath = cmdline.getOptionValue(INDEX);
    String collectionPath = cmdline.getOptionValue(COLLECTION);

    if (collectionPath.endsWith(".gz")) {
      System.out.println("gzipped collection is not seekable: use compressed version!");
      System.exit(-1);
    }

    Configuration config = new Configuration();
    FileSystem fs = FileSystem.get(config);
    MapFile.Reader reader = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), config);

    FSDataInputStream collection = fs.open(new Path(collectionPath));
    BufferedReader d = new BufferedReader(new InputStreamReader(collection));

    Text key = new Text();
    // should change this to BytesWritable
    BytesWritable value = new BytesWritable();

    // the merely for the purpose of printing exactly like the previous
    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value_to_print = new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();
    IntWritable df_to_print = new IntWritable();
    ArrayListWritable<PairOfInts> postings_to_print = new ArrayListWritable<PairOfInts>();

    // STEP1: search for starcross'd"
    System.out.println("Looking up postings for the term \"starcross'd\"");
    key.set("starcross'd");

    reader.get(key, value);

    // deserialize the value which is DF d1:f1, d2:f2, d3:f3.....
    // d1 is docid1, d2 is docid2-docid1.....
    // we need to retrieve these value
    ByteArrayInputStream bais = new ByteArrayInputStream(value.getBytes());
    DataInputStream dis = new DataInputStream(bais);

    // read df
    int df = WritableUtils.readVInt(dis);

    // scan through postings (value)
    int gap = 0;
    int tf = 0;
    postings_to_print.clear();
    PairOfInts pair = null;// because I don't have any idea what System.out.println(pair), need to
                           // this
    for (int i = 0; i < df; i++) {
      gap += WritableUtils.readVInt(dis);
      tf = WritableUtils.readVInt(dis);
      pair = new PairOfInts(gap, tf);
      System.out.println(pair);
      collection.seek(gap);
      System.out.println(d.readLine());
    }
    bais.reset();
    dis.close();

    // STEP2: search gold
    key.set("gold");
    reader.get(key, value);
    // cant' print this because different format
    // System.out.println("Complete postings list for 'gold': " + value);

    Int2IntFrequencyDistribution goldHist = new Int2IntFrequencyDistributionEntry();
    bais = new ByteArrayInputStream(value.getBytes());
    dis = new DataInputStream(bais);
    // again, get doc frequency
    df = WritableUtils.readVInt(dis);
    df_to_print.set(df);
    gap = 0;
    tf = 0;
    postings_to_print.clear();
    for (int i = 0; i < df; i++) {
      gap += WritableUtils.readVInt(dis);
      tf = WritableUtils.readVInt(dis);
      postings_to_print.add(new PairOfInts(gap, tf));
      goldHist.increment(tf);
    }
    value_to_print = new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(df_to_print,
        postings_to_print);
    System.out.println("Complete postings list for 'gold': " + value_to_print);
    System.out.println("histogram of tf values for gold");
    for (PairOfInts pai : goldHist) {
      System.out.println(pai.getLeftElement() + "\t" + pai.getRightElement());
    }
    bais.reset();
    dis.close();

    // STEP3: find silver
    key.set("silver");
    reader.get(key, value);
    // can't print this either
    // System.out.println("Complete postings list for 'silver': " + value);

    Int2IntFrequencyDistribution silverHist = new Int2IntFrequencyDistributionEntry();
    bais = new ByteArrayInputStream(value.getBytes());
    dis = new DataInputStream(bais);
    // again, get doc frequency
    df = WritableUtils.readVInt(dis);
    df_to_print.set(df);
    gap = 0;
    tf = 0;
    postings_to_print.clear();
    for (int i = 0; i < df; i++) {
      gap += WritableUtils.readVInt(dis);
      tf = WritableUtils.readVInt(dis);
      postings_to_print.add(new PairOfInts(gap, tf));
      silverHist.increment(tf);
    }

    value_to_print = new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(df_to_print,
        postings_to_print);
    System.out.println("Complete postings list for 'silver': " + value_to_print);
    System.out.println("histogram of tf values for silver");
    for (PairOfInts pai : silverHist) {
      System.out.println(pai.getLeftElement() + "\t" + pai.getRightElement());
    }
    bais.reset();
    dis.close();
    // STEP4: serach for bronze, no bronze
    key.set("bronze");
    Writable w = reader.get(key, value);

    if (w == null) {
      System.out.println("the term bronze does not appear in the collection");
    }

    collection.close();
    reader.close();

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new LookupPostingsCompressed(), args);
  }
}
