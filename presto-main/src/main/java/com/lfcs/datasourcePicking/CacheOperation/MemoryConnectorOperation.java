package com.lfcs.datasourcePicking.CacheOperation;

import com.lfcs.datasourcePicking.ViewManagement.View;
import com.lfcs.datasourcePicking.ViewManagement.ViewManagement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/*
insert delete select update memory connector
 */
public class MemoryConnectorOperation {

  CacheOperation cacheOperation;

  int capacity;

  ViewManagement viewManagement;

  //connectionURL
  String url;

  Connection connection;

  public MemoryConnectorOperation(CacheOperation cacheOperation, int capacity,
                                  ViewManagement viewManagement, String url) {
    this.capacity = capacity;
    this.viewManagement = viewManagement;
    this.url = url;

    try {
      Class.forName("com.facebook.presto.jdbc.PrestoDriver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    try {
      this.connection = DriverManager.getConnection(url);
    } catch (SQLException e) {
      e.printStackTrace();
    }

  }

  //directly handle cache
  /**
   * Where should we call this method?
   * How to construct a SQL command (view object)
   * Add a view into the cache , check if the size of the cache is enough given that we are in
   * LRU mode. After adding a view into the cache, we should update the view index
   * SQL format: Create table as select xxxxxx
   */
  public void addView(SqlCommand command){
    String sql = command.sql;
    try {
      Statement statement = connection.createStatement();
      // TODO: here we check if the size of the memory is enough
      if(true){
        View view = evictView();
        viewManagement.dropViews(view);
      }
      //actually store
      statement.execute(sql);
      //update the LRU doubly linked list
      cacheOperation.addView(command.view);

      //update the view Index
      viewManagement.storeView(command.view);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * If the capacity is not enough, we should evict this view
   */
  private View evictView(){
    return cacheOperation.remove();
  }

}
