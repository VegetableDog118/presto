package com.lfcs.datasourcePicking.ViewManagement;

import java.sql.Timestamp;
import java.util.List;

public class View implements Comparable{
  //1.whole select:SQL select * from a join b on a.e = b
  //view-name;X   connector table time:X+time
  //2.join table t1(cache) join t2(DB) -> index (cache)t1?
  //sub query (select)1
  String viewName;// t

  String tableName;// store in memory t-1.1 t-1.2 t-1.3

  //column name of the
  List<Column> cols;

  //Version
  Timestamp timestamp;

  //For view with same name, it will store in set by it's timestamped order
  @Override
  public int compareTo(Object o) {
    if(!(o instanceof View)){
      throw new RuntimeException("not a view");
    }
    View view= (View) o;
    if(this.timestamp.getTime()>view.timestamp.getTime()){
      return 1;
    }else{
      return -1;
    }
  }

  @Override
  public String toString() {
    return "View{" +
        "viewName='" + viewName + '\'' +
        ", cols=" + cols +
        ", timestamp=" + timestamp +
        '}';
  }

  public String getViewName() {
    return viewName;
  }

  public String getTableName() {
    return tableName;
  }

  public Timestamp getTimestamp() {
    return timestamp;
  }


  public View(String viewName, String tableName,  Timestamp timestamp) {
    this.viewName = viewName;
    this.tableName = tableName;
    this.timestamp = timestamp;
  }
}
