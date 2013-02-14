import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Set;
import java.util.TreeMap;

public class GetCloudLovePMI {
  // public static final double docLog = 10.00;

  public static void main(String[] args) throws Exception {
    try {
      // Open the file that is the first
      // command line parameter
      String inputFile = args[0];
      // String outputFile = args[1];
      FileInputStream fstream = new FileInputStream(inputFile);
      // Get the object of DataInputStream
      DataInputStream in = new DataInputStream(fstream);
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      String strLine;

      // Read File Line By Line
      TreeMap<Double, String> cloudPMI = new TreeMap<Double, String>();
      TreeMap<Double, String> lovePMI = new TreeMap<Double, String>();
      //
      // String pair;
      double small = 0.00000000001;
      double pmi = 0.0;
      double count = 0.0;
      while ((strLine = br.readLine()) != null) {
        String[] itr = strLine.split("\\t");// hope that file is tab deliminated
        if (itr.length == 2) {
          pmi = Double.parseDouble(itr[1]);
          if (itr[0].startsWith("(love, ")) {

            lovePMI.put((small * count++) - pmi, itr[0]);
          }
          if (itr[0].startsWith("(cloud, ")) {

            cloudPMI.put((small * count++) - pmi, itr[0]);
          }

          // pair = itr[0];
          // pmi = Double.parseDouble(itr[1]);
          // if (pairPMI.containsKey(pair)) {
          // pairPMI.put(pair, pairPMI.get(pair) + pmi);
          // } else {
          // pairPMI.put(pair, pmi);
          // }
        }

      }

      int limit = 3;
      Set<Double> loveSet = lovePMI.keySet();
      for (double d : loveSet) {
        if (limit > 0) {
          System.out.println(d + "\t" + lovePMI.get(d));
          limit--;
        }
      }

      limit = 3;
      Set<Double> cloudSet = cloudPMI.keySet();
      for (double d : cloudSet) {
        if (limit > 0) {
          System.out.println(d + "\t" + cloudPMI.get(d));
          limit--;
        }
      }

      // Close the input stream
      in.close();

    } catch (Exception e) {// Catch exception if any
      System.err.println("Error: " + e.getMessage());
    }

  }

  // write
  public static void write(String s, String fileName, boolean append) {

    try {

      FileWriter outStream = new FileWriter(fileName, append);

      outStream.write(s);

      outStream.close();

    } catch (UnsupportedEncodingException e) {

    }

    catch (IOException e) {

      System.out.println("IOERROR: " + e.getMessage() + "\n");

      e.printStackTrace();

    }

  }
}
