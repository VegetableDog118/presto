package com.lfcs.datasourcePicking.CacheOperation;

import com.lfcs.datasourcePicking.ViewManagement.View;

public class SqlCommand {
  String sql;// create table as select

  String createSql;

  String selectSql;

  View view;

  public SqlCommand(String sql, String createSql, String selectSql, View view) {
    this.sql = sql;
    this.createSql = createSql;
    this.selectSql = selectSql;
    this.view = view;
  }
}
