package com.lfcs.datasourcePicking.ViewManagement;

import java.nio.file.DirectoryStream;
import java.util.List;
import java.util.Optional;

public class Column {

  String colName;//age
  List<Filter> filters; // 10 < age , age < 20

  public Column(String colName, List<Filter> filters) {
    this.colName = colName;
    this.filters = filters;
  }


  public class Filter{

    LogicalOp op;

    String col;

    String value;

    public Filter(LogicalOp op, String col, String equalVal) {
      this.op = op;
      this.col = col;
      this.value = equalVal;
    }



  }
  //+2
  enum LogicalOp{
    Greater,
    Smaller,
    Equal
  }
}
