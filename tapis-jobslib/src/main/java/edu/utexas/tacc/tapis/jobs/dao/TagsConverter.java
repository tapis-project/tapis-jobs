package edu.utexas.tacc.tapis.jobs.dao;


import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.TreeSet;
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

  public static TreeSet<String> fromJDBCArray(Array dbArray) throws SQLException {
    if (dbArray == null) return null;
    String[] sa = (String[]) dbArray.getArray();
    return new TreeSet<>(Arrays.asList(sa));
  }
  
  public Class<String[]> fromType() { return String[].class; }
  public Class<TreeSet> toType() { return TreeSet.class; }
}