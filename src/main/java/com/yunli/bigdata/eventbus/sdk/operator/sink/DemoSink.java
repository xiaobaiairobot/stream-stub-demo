package com.yunli.bigdata.eventbus.sdk.operator.sink;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yunli.bigdata.eventbus.sdk.message.ColumnDesc;
import com.yunli.bigdata.eventbus.sdk.message.DefaultRowSet;
import com.yunli.bigdata.eventbus.sdk.message.Row;
import com.yunli.bigdata.eventbus.sdk.message.RowSet;

/**
 * @author david
 * @date 2020/8/19 5:40 下午
 */
public class DemoSink extends AbstractSink {

  private final Logger logger = LoggerFactory.getLogger(DemoSink.class);

  public DemoSink(Map<String, Object> config) {
    // deal with config
  }

  @Override
  public void invoke(RowSet rowSet) throws Exception {
    DefaultRowSet defaultRowSet = (DefaultRowSet) rowSet;
    List<ColumnDesc> columns = defaultRowSet.getSchema().getColumns();
    for (Row row : defaultRowSet) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < columns.size(); i++) {
        sb.append(String.format("%s:%s,", columns.get(i).getColumnName(), row.get(i)));
      }
      if (sb.length() > 0) {
        logger.info(sb.delete(sb.length() - 1, sb.length()).toString());
      } else {
        // 空行
        logger.info(sb.delete(sb.length() - 1, sb.length()).toString());
      }
    }
  }

  @Override
  public void destroy() {
    System.exit(1);
  }

  @Override
  public void close() {

  }
}
