package common;

/**
 * Created by dakle on 8/4/17.
 */
public class Utils {

    /** return the DakleBase VERSION */
    public static String getVersion() {
        return Constants.VERSION;
    }

    public static String getCopyright() {
        return Constants.COPYRIGHT;
    }

    public static void displayVersion() {
        System.out.println("DakleBaseLite Version " + getVersion());
        System.out.println(getCopyright());
    }

    /**
     * @param s The String to be repeated
     * @param num The number of time to repeat String s.
     * @return String A String object, which is the String s appended to itself num times.
     */
    public static String line(String s, int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }

}
