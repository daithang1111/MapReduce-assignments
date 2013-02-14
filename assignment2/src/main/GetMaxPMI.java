import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Set;

public class GetMaxPMI {
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
      // HashMap<String, Double> pairPMI = new HashMap<String, Double>();
      //
      // String pair;
      double maxPMI = -10000.0;
      double pmi = -10000.0;
      String maxPairs = "";
      while ((strLine = br.readLine()) != null) {
        String[] itr = strLine.split("\\t");// hope that file is tab deliminated
        if (itr.length == 2) {
          pmi = Double.parseDouble(itr[1]);
          if (maxPMI < pmi) {
            maxPMI = pmi;
            maxPairs = itr[0];
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
      System.out.println(maxPairs+"\t"+maxPMI);
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
