package edu.utexas.tacc.tapis.jobs.dao;


import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.jooq.Converter;
@SuppressWarnings({ "serial", "rawtypes" })
public class TagsConverter implements Converter<String[], TreeSet>
{
  public TreeSet<String> from(String[] sa)
  {
    return new TreeSet<>(Arrays.asList(sa));
  }
  public String[] to(TreeSet ts)
  {
    return (String[]) ts.toArray();
  }
  
  /**
   * Convert a TreeSet<String> to a JDBC Array.
   * Since this is particularly for Tags, we always ensure that it returns a non-null
   * (but possibly empty) Array.
   * @param conn
   * @param tags
   * @return
   * @throws SQLException
   */
  public static Array toJDBCArray(Connection conn, TreeSet<String> tags) throws SQLException {
    Array tagsArray;
    if (tags == null || tags.isEmpty())
      tagsArray = conn.createArrayOf("text", new String[0]);
    else {
      String[] sarray = tags.toArray(new String[tags.size()]);
      tagsArray = conn.createArrayOf("text", sarray);
    }
    return tagsArray;
  }
  /**
   * Convert a JDBC Array to a TreeSet<String>. 
   * Since this is particularly for Tags, we always ensure that it returns a non-null
   * (but possibly empty) TreeSet.
   * @param dbArray
   * @return
   * @throws SQLException
   */
  public static TreeSet<String> fromJDBCArray(Array dbArray) throws SQLException {
    if (dbArray == null) return new TreeSet<>();
    String[] sa = (String[]) dbArray.getArray();
    return new TreeSet<>(Arrays.asList(sa));
  }
  
  public Class<String[]> fromType() { return String[].class; }
  public Class<TreeSet> toType() { return TreeSet.class; }
}