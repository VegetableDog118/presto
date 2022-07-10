package com.lfcs.datasourcePicking.CacheOperation;

import com.lfcs.datasourcePicking.ViewManagement.View;

public interface CacheOperation {

  //remove the view if the memory size is not enough
  View remove();

  //add a view into the cache
  void addView(View view);

}
