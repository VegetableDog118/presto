package com.lfcs.datasourcePicking.ViewManagement;

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;

public class ViewManagement {

  //TODO:singleton

  // index
  ViewManagementIndex viewIndex = ViewManagementIndex.getInstance();

  int cacheSize = 0;
  private Session session;
  private Metadata metadata;
  private PlanVariableAllocator variableAllocator;
  //private



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


  private void changeSource(PlanNode parent, PlanNode leaf){
    //call view
    ConnectorId connectorId = new ConnectorId("memory");


    List<PlanNode> newPlanNode = new ArrayList<>();
    TableHandle targetTable = null;

    Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTable);
    ImmutableList.Builder<VariableReferenceExpression> tableScanOutputsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> variableToColumnHandle = ImmutableMap.builder();
    TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTable);
    for (ColumnMetadata column : tableMetadata.getColumns()) {
      VariableReferenceExpression variable = variableAllocator.newVariable(leaf.getSourceLocation(), column.getName(), column.getType());
      tableScanOutputsBuilder.add(variable);
      variableToColumnHandle.put(variable, columnHandles.get(column.getName()));
    }
    List<VariableReferenceExpression> tableScanOutputs = tableScanOutputsBuilder.build();
    TableScanNode tableScanNode = new TableScanNode(leaf.getSourceLocation(), leaf.getId(), targetTable, tableScanOutputs, variableToColumnHandle.build(), TupleDomain.all(), TupleDomain.all());
    newPlanNode.add(tableScanNode);
    parent.replaceChildren(newPlanNode);
  }

  public void setViewManagement(Session session, Metadata metadata, PlanVariableAllocator variableAllocator) {
    this.session = session;
    this.metadata = metadata;
    this.variableAllocator = variableAllocator;
    //this.analysis = null;
  }

}
