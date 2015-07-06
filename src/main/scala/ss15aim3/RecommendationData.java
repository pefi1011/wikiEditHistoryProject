package ss15aim3;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public class RecommendationData {

    public static final String[] ITEMS = new String[] {

            "REVISION 4781981 72390319 Steven_Strogatz 2006-08-28T14:11:16Z SmackBot 433328\n" +
                    "CATEGORY American_mathematicians\n" +
                    "IMAGE\n" +
                    "MAIN Boston_University MIT Harvard_University Cornell_University\n" +
                    "TALK\n" +
                    "USER\n" +
                    "USER_TALK\n" +
                    "OTHER De:Steven_Strogatz Es:Steven_Strogatz\n" +
                    "EXTERNAL http://www.edge.org/3rd_culture/bios/strogatz.html\n" +
                    "TEMPLATE Cite_book Cite_book Cite_journal\n" +
                    "COMMENT ISBN formatting &/or general fixes using [[WP:AWB|AWB]]\n" +
                    "MINOR 1\n" +
                    "TEXTDATA 229\n" +
                    "\n" +
                    "REVISION 2343225 53232523 Kiro_skalata 2000-06-21T05:10:33Z NoBot 65728\n" +
                    "CATEGORY Astrophysik\n" +
                    "IMAGE\n" +
                    "MAIN KIT HS-Karlsruhe\n" +
                    "TALK\n" +
                    "USER\n" +
                    "USER_TALK\n" +
                    "OTHER De:Steven_Strogatz Es:Steven_Strogatz\n" +
                    "EXTERNAL www.aksalbereinstein.com\n" +
                    "TEMPLATE Cool_BOOK One_more\n" +
                    "COMMEN     T ISBN formatting &/or general fixes using [[WP:AWB|AWB]]\n" +
                    "MINOR 0\n" +
                    "TEXTDATA 1000"

    };

    public static DataSet<String> getDefaultTextLineDataSet(ExecutionEnvironment env) {
        return env.fromElements(ITEMS);
    }
}
