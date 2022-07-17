package com.lfcs.datasourcePicking.CacheOperation;

import com.lfcs.datasourcePicking.ViewManagement.View;
import com.lfcs.datasourcePicking.ViewManagement.ViewManagement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

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
    this.cacheOperation = cacheOperation;

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
  /**sd
   * Where should we call this method?
   * How to construct a SQL command (view object)
   * Add a view into the cache , check if the size of the cache is enough given that we are in
   * LRU mode. After adding a view into the cache, we should update the view index
   * SQL format: Create table as select xxxxxx
   */
  public void addView(String query){
    SqlCommand command = generateSqlCommand(query);
    String sql = command.sql;
    try {
      Statement statement = connection.createStatement();
      // TODO: here we check if the size of the memory is enough
      if(true){
        View view = evictView();
        viewManagement.dropViews(view);
        deleteView(view);
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

  private void deleteView(View view){
    String viewName = view.getTableName();
    String command = "String deleteCommand = DROP TABLE memory.default." + viewName;
    try {
      Statement statement = connection.createStatement();
      statement.execute(command);
    }catch (SQLException e) {
      e.printStackTrace();
    }
  }



  private SqlCommand generateSqlCommand(String query){
    String tableName = generateTableName(query);
    String create = "CREATE TABLE memory.default." + tableName;
    String select = query;
    String command = create + "AS" + select;
    Timestamp ts = new Timestamp(System.currentTimeMillis());
    View view = new View(generateViewName(query,ts),tableName,ts);
    SqlCommand sqlCommand = new SqlCommand(command,create,query,view);
    return sqlCommand;
  }

  /**
   * Select a , b, c from t Table-abc
   * @param sql
   * @return
   */
  private String generateTableName(String sql){


    
    String[] str = sql.split(" ");
    StringBuilder stringBuilderBuilder = new StringBuilder();
    boolean add = false;
    for(String s : str){
      if(s.toLowerCase().equals("select")){
        add = true;
      }else if(s.toLowerCase().equals("from")){
        add = false;
      }
      if(add){
        stringBuilderBuilder.append(s);
      }
    }
    return "Table-"+stringBuilderBuilder.toString();
  }

  private String generateViewName(String sql, Timestamp ts){
    //Timestamp ts = new Timestamp(System.currentTimeMillis());
    return generateTableName(sql) + ts;
  }


}
