package com.lfcs.datasourcePicking.ViewManagement;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.lfcs.datasourcePicking.CacheOperation.MemoryConnectorOperation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class ViewManagement {

  //TODO:singleton

  //Catlog Manager
  CatalogManager catalogManager;


  private static ViewManagement instance =  new ViewManagement();

  MemoryConnectorOperation memoryConnectorOperation;

  private ViewManagement getInstance(){
    return instance;
  }

  // index
  ViewManagementIndex viewIndex = ViewManagementIndex.getInstance();

  /**
   * This method is called when we have insert cache operation in Memory Connector Operation
   * @param view
   * @return
   */
  public boolean storeView(View view){
    boolean success =viewIndex.storeView(view);
    return success;
  }

  /**
   * Update index when we drop a view in cache operation.
   * @param view
   * @return
   */
  public boolean dropViews(View view){
    return viewIndex.dropViews(view.getTableName());
  }

  /**
   * Use to support our algorithm decision
   * @param viewName
   * @return
   */
  public TreeSet<View> getViews(String viewName){
    return viewIndex.getViews(viewName);

  }
  // Algorithm: pick view
  public PlanNode pickDatasource(PlanNode root){
    //子节点和parent
    Map<PlanNode,PlanNode> leafParentMap = new HashMap<>();
    //level of nodes
    List<PlanNode> nodes = root.getSources();
    //leaf nodes
    List<PlanNode> leaf = new ArrayList<>();
    //?
    for(PlanNode node: nodes){
      leafParentMap.put(node,root);
    }

    //TODO correct?
    while(!nodes.equals(Collections.emptyList())){
        List<PlanNode> nextLayer = Collections.emptyList();
        //nodes is a layer of node which might be a leaf node
        for(PlanNode node: nodes){
          List<PlanNode> children = node.getSources();
          for(PlanNode child: children){
            leafParentMap.put(child,node);
          }
          if(children.equals(Collections.emptyList())){
            //leaf node
            //
            leaf.add(node);
          }else{
            nextLayer.addAll(children);
          }
        }
        nodes = nextLayer;
    }

    //
    for(PlanNode l : leaf){
      changeSource(leafParentMap.get(l),l);
    }

    return root;
  }


  private void changeSource(PlanNode parent,PlanNode leaf){

    //call view

  }


  public ViewManagement() {

  }
}
