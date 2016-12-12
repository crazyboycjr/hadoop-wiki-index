package xyz.cjr.wiki_index;

/**
 * Created by cjr on 12/12/16.
 */

import org.apache.hadoop.util.ProgramDriver;

/**
 * A description of an example program based on its class and a
 * human-readable description.
 */
public class ExampleDriver {

  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("WikiTitleExtract", WikiTitleExtract.class,
              "A map/reduce program that extract the contents between <title></title> in xml files");

      pgd.addClass("WikiWordCount", WikiWordCount.class,
                   "A map/reduce program that count the word frequency in the files");

      pgd.addClass("BadDocCount", BadDocCount.class,
                "A map/reduce program that count the document which do not contains <title></title>");

      pgd.addClass("InvertedIndex", InvertedIndex.class,
              "A map/reduce program that calculate the inverted index");
      exitCode = pgd.run(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }

    System.exit(exitCode);
  }
}
