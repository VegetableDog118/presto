package com.lfcs.datasourcePicking.ViewManagement;

import com.facebook.presto.spi.plan.PlanNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ViewManagement {

  //TODO:singleton

  // index
  ViewManagementIndex viewIndex = ViewManagementIndex.getInstance();

  int cacheSize = 0;

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

}
