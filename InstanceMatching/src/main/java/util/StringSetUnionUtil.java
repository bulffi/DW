package util;

import java.util.*;

/**
 * @program: hadoop
 * @description: To union several string sets into one
 * @author: Zijian Zhang
 * @create: 2019/11/23
 **/
public class StringSetUnionUtil {
  public static List<String> getUnion(String[] values) {
    Set<String> strings = new HashSet<String>();
    for (String s :
      values) {
      if (s.startsWith("\"")) {
        s = s.substring(1);
      }
      if (s.endsWith("\"")) {
        s = s.substring(0, s.length() - 1);
      }
      String[] subs = s.split(", ");
      if(subs.length==1 && subs[0].contains("; ")){
        subs = s.split("; ");
      }
      for (String sub :
        subs) {
        if (!sub.equals("") && !sub.equals(" ")) {
          strings.add(sub);
        }
      }
    }
    return new ArrayList<String>(strings);
  }


  public static void main(String[] args){
    List<String> temt = getUnion(new String[]{"\"Blu-ray, DVD\"","sd; sad","","DVD","Audio CD"});
    List<String> tempt = getUnion(new String[]{"\"Jeff Fahey, Erika Eleniak, Bill Dow, Jessica Amlee, Michael Ryan\"","\"Brittany Konarzewski, Wesley Barker, Herbie Baez, Marshal Hilton, Terence J. Rotolo\"","zzj"});
  }
}
