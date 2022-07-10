package com.lfcs.datasourcePicking.ViewManagement;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class ViewManagementIndex {

  private static ViewManagementIndex instance = new ViewManagementIndex();

  private ConcurrentHashMap<String, TreeSet<View>> viewMap = new ConcurrentHashMap<>();

  private ConcurrentHashMap<String, String> tableNameViewMap = new ConcurrentHashMap<>();

  public static ViewManagementIndex getInstance(){
    return instance;
  }


  //call this method when we store a
  public boolean storeView(View view){
    if(!tableNameViewMap.containsKey(view.getTableName())){
      tableNameViewMap.put(view.getTableName(), view.getViewName());
    }
    if(viewMap.containsKey(view.getViewName())){
      viewMap.get(view.getViewName()).add(view);
    }else{
      TreeSet<View> views = new TreeSet<>();
      views.add(view);
      viewMap.put(view.getViewName(), views);
    }
    return true;
  }

  /**
   * call when view management algorithms need to check views
   * @param viewName
   * @return
   */
  public TreeSet<View> getViews(String viewName){
    if(viewMap.containsKey(viewName)){
      return viewMap.get(viewName);
    }else{
      return null;
    }
  }

  /**
   * When memory connector drop a table, we need to update our view index
   * @param tableName
   * @return
   */
  public boolean dropViews(String tableName){
    //table -> view ->set -> delete
    String viewName = "";
    if(tableNameViewMap.contains(tableName)){
      viewName = tableNameViewMap.get(tableName);
    }
    Set<View> views = viewMap.get(viewName);
    Iterator<View> iter = views.iterator();
    while(iter.hasNext()){
      View v = iter.next();
      if(v.getTableName().equals(tableName)){
        iter.remove();
        break;
      }
    }
    return true;
  }
}
