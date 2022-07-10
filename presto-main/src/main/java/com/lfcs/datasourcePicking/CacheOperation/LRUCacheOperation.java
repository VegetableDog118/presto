package com.lfcs.datasourcePicking.CacheOperation;

import com.lfcs.datasourcePicking.ViewManagement.View;

import java.util.HashMap;
import java.util.Map;

public class LRUCacheOperation implements CacheOperation{

  //hashmap + double linked list
  //key table name,
  Map<String , DNode> hashmap = new HashMap();
  DLLHelper helper;

  public LRUCacheOperation() {
    this.helper = new DLLHelper();
  }

  @Override
  public View remove() {
      //cache operation delete:
    View delView = this.helper.head.view;
    hashmap.remove(this.helper.head.key);
    helper.removeFromFront(this.helper.head);
    return delView;
  }

  //ToDO: where to call?
  public void beenUse(View view){
    DNode node = hashmap.get(view.getTableName());
    helper.removeFromFront(node);
    helper.addToTail(node);
  }

  // create
  @Override
  public void addView(View view) {
    DNode cur = hashmap.get(view.getTableName());
    if(cur!=null){
      helper.addToTail(cur);
    }
  }


  class DNode{
    //table name of the view
    String key;
    //View class
    View view;
    DNode prev;
    DNode next;

    public DNode(String key, View view) {
      this.key = key;
      this.view = view;
      this.prev = null;
      this.next = null;
    }

  }

  class DLLHelper{
    DNode head;
    DNode tail;

    DLLHelper(){

    }
    //add element to Tail
    public void addToTail(DNode cur){
      //1->2->3
      //      (add)

      if(this.tail!=null){
        this.tail.next = cur;
      }
      //set cur node prev to previous tail (last node)
      cur.prev = this.tail;
      cur.next = null;//important to set next node to null.
      this.tail = cur;//set cur as tail node.


      //if add node is first node
      //since null object cannot refer and track other object
      //sync first and point both (head==>tail=cur)
      if(head==null){
        this.head = this.tail;
      }
    }

    public void removeFromFront(DNode cur){
      //1->2->3
      //   X(remove)
      //if node to remove is in middle.

      //to remove middle node
      //1. first remove prev ref and next reference to cur node(to be removed)
      if(cur.prev!=null){
        cur.prev.next = cur.next;
      }else{
        this.head = cur.next;
      }

      if(cur.next!=null){
        cur.next.prev = cur.prev;
      }else{
        this.tail = cur.prev;
      }

      //if suppose we remove only one existing node.
      //sync tail to null.
      if(this.head==null){
        this.tail = null;
      }
    }
  }
}
