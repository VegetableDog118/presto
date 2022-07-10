package com.facebook.datasourcePicking.CacheOperation;

public interface CacheOperation {

  //remove the view if the memory size is not enough
  void remove();

  //add a view into the cache
  void addView();

}
