package com.lfcs.datasourcePicking.CacheOperation;

import com.facebook.presto.sql.tree.CurrentTime;
import com.lfcs.datasourcePicking.ViewManagement.View;
import com.lfcs.datasourcePicking.ViewManagement.ViewManagement;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.util.TablesNamesFinder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.sf.jsqlparser.parser.feature.Feature.select;

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
      if(false){
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


  public void addBaseTable(String query){
    List<String> tables = resolveBaseTable(query);
    for(String t : tables){
      String sql = "select * from " + t + ";";
      addView(sql);
    }
  }



  private SqlCommand generateSqlCommand(String query){
    String viewName = generateViewName(query);
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    String tableName = generateTableNameInConnector(viewName,timestamp);
    String create = "CREATE TABLE memory.default." + tableName;
    String select = query;
    String command = create + "AS" + select;
    Timestamp ts = new Timestamp(System.currentTimeMillis());

    //resolve columns
    List<String> cols = resolveCol(query);
    View view = new View(viewName,tableName,ts);
    SqlCommand sqlCommand = new SqlCommand(command,create,query,view);
    return sqlCommand;
  }


  /**
   * Select a , b, c from t1 join t2 on
   * Then the table name stored in meme Table-T1T2-timestamped
   * @param
   * @return
   */
  private String generateTableNameInConnector(String viewName, Timestamp ts)  {
    return viewName+ts;
  }


  private String generateViewName(String sql){

    List<String> tableList = resolveBaseTable(sql);
    StringBuilder stringBuilderBuilder = new StringBuilder();
    for(String table : tableList){
      stringBuilderBuilder.append(table);
    }
    return "Table-"+stringBuilderBuilder;
  }

  private List<String> resolveBaseTable(String sql){
    net.sf.jsqlparser.statement.Statement statement = null;
    try {
      statement = CCJSqlParserUtil.parse(sql);
    } catch (JSQLParserException e) {
      e.printStackTrace();
    }
    Select selectStatement = (Select) statement;
    TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
    List<String> tableList = tablesNamesFinder.getTableList(selectStatement);
    return tableList;
  }


  private List<String> resolveCol(String sql){
    Map<String, Expression> map = new HashMap<>();
    Select stmt = null;
    try {
      stmt = (Select) CCJSqlParserUtil.parse(sql);
    } catch (JSQLParserException e) {
      e.printStackTrace();
    }
    for (SelectItem selectItem : ((PlainSelect)stmt.getSelectBody()).getSelectItems()) {
      selectItem.accept(new SelectItemVisitorAdapter() {
        @Override
        public void visit(SelectExpressionItem item) {
          map.put(item.getAlias().getName(), item.getExpression());
        }
      });
    }
    return new ArrayList<>(map.keySet());
  }
}
