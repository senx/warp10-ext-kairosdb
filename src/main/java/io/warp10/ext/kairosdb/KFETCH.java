//
//   Copyright 2020  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.ext.kairosdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Aggregator;
import org.kairosdb.client.builder.DataPoint;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.QueryMetric;
import org.kairosdb.client.builder.QueryMetric.Order;
import org.kairosdb.client.builder.aggregator.CustomAggregator;
import org.kairosdb.client.builder.grouper.TagGrouper;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.QueryResult;
import org.kairosdb.client.response.Result;

import com.google.gson.internal.LazilyParsedNumber;

import io.warp10.continuum.TimeSource;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.json.JsonUtils;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class KFETCH extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  private static final String PARAM_URL = "url";
  private static final String PARAM_END = "end";
  // This is a map of metric name to list of tag maps
  private static final String PARAM_METRICS = "metrics";
  
  private static final String PARAM_START = "start";
  private static final String PARAM_LIMIT = "limit";
  private static final String PARAM_ORDER = "order";
  private static final String PARAM_GROUPBY = "groupby";  
  private static final String PARAM_AGGREGATORS = "aggregators";
  private static final String AGGREGATOR_NAME = "name";
    
  public KFETCH(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expects a parameter map.");
    }
    
    Map<Object,Object> params = (Map<Object,Object>) top;
    
    String url = URLValidator.validate(String.valueOf(params.getOrDefault(PARAM_URL, "")));
    
    Object param = params.getOrDefault(PARAM_END, TimeSource.getTime());
    
    if (!(param instanceof Long)) {
      throw new WarpScriptException(getName() + " expects parameter '" + PARAM_END + "' to be a LONG.");
    }
    
    Date end = new Date(((Long) param).longValue() / Constants.TIME_UNITS_PER_MS);
    
    param = params.get(PARAM_START);
    
    if (!(param instanceof Long)) {
      throw new WarpScriptException(getName() + " expects parameter '" + PARAM_START + "' to be a LONG.");
    }
    
    Date start = new Date(((Long) param).longValue() / Constants.TIME_UNITS_PER_MS);
    
    param = params.get(PARAM_METRICS);
    
    if (!(param instanceof Map)) {
      throw new WarpScriptException(getName() + " expects parameter '" + PARAM_METRICS + "' to be a MAP.");
    }
    
    Map<Object,Object> metrics = (Map<Object,Object>) param;
    
    param = params.get(PARAM_LIMIT);
    
    Integer limit = null;
    
    if (null != param) {
      limit = ((Number) param).intValue();
    }
    
    param = params.get(PARAM_ORDER);
    
    Order order = null;
    
    if (param instanceof String) {
      if ("desc".equals(param)) {
        order = Order.DESCENDING;
      } else if ("asc".equals(param)) {
        order = Order.ASCENDING;
      } else {
        throw new WarpScriptException(getName() + " expected either 'asc' or 'desc' as '" + PARAM_ORDER + "' parameter.");
      }
    }
    
    param = params.get(PARAM_GROUPBY);
    
    List<String> groupbyparam = new ArrayList<String>();
    
    if (param instanceof List) {
      for (Object elt: (List) param) {
        groupbyparam.add(String.valueOf(elt));
      }
    }
    
    List<Aggregator> aggregators = new ArrayList<Aggregator>();
    param = params.get(PARAM_AGGREGATORS);
    
    if (param instanceof List) {
      for (Object elt: (List) param) {
        if (!(elt instanceof Map)) {
          throw new WarpScriptException(getName() + " aggregators are expected to be MAPs.");
        }
        Map<Object,Object> map = (Map<Object,Object>) elt;
        
        if (!map.containsKey(AGGREGATOR_NAME)) {
          throw new WarpScriptException(getName() + " aggregator MAP must contain key '" + AGGREGATOR_NAME + "'.");
        }

        String name = String.valueOf(map.get(AGGREGATOR_NAME));

        try {
          HashMap<Object,Object> noname = new HashMap<Object,Object>(map);
          noname.remove(AGGREGATOR_NAME);
          String json = JsonUtils.objectToJson(noname);
          json = json.substring(1).substring(0, json.length() - 2);
          // The following is rather hacky, since CustomAggregator expects a non
          // empty json, we repeat the name key+value so we do not add an unexpected key which
          // cannot be deserialized but simply repeat one that is expected.
          if (json.isEmpty()) {
            json = "'name':'" + name + "'";
          }
          Aggregator aggregator = new CustomAggregator(name, json);           
          aggregators.add(aggregator);
        } catch (IOException ioe) {
          throw new WarpScriptException(getName() + " error while serializing aggregator.", ioe);
        }
      }
    }

    try {
      try(HttpClient client = new HttpClient(url)) {
        QueryBuilder builder = QueryBuilder.getInstance();
        builder.setStart(start).setEnd(end);

        for (Entry<Object,Object> entry: metrics.entrySet()) {          
          if (!(entry.getKey() instanceof String)) {
            throw new WarpScriptException(getName() + " expects the keys of the MAP '" + PARAM_METRICS + "' to be STRINGs.");
          }
          
          if (!(entry.getValue() instanceof List) && !(entry.getValue() instanceof Map)) {
            throw new WarpScriptException(getName() + " expects the values of the MAP '" + PARAM_METRICS + "' to be MAPs or LISTs of MAPs.");
          }
          
          List<Object> taglist;
          
          if (entry.getValue() instanceof List) {
            taglist = (List<Object>) entry.getValue();
          } else {
            taglist = new ArrayList<Object>(1);
            taglist.add(entry.getValue());
          }
          
          for (Object tags: taglist) {
            if (!(tags instanceof Map)) {
              throw new WarpScriptException(getName() + " expects the values of the MAP '" + PARAM_METRICS + "' to be MAPs or LISTs of MAPs.");              
            }

            QueryMetric qmetric = builder.addMetric((String) entry.getKey());
            
            if (null != limit) {
              qmetric.setLimit(limit.intValue());
            }
            
            if (null != order) {
              qmetric.setOrder(order);
            }

            Set<String> groupby = new HashSet<String>(groupbyparam);
            for (Entry<Object,Object> tag: ((Map<Object,Object>) tags).entrySet()) {
              if (tag.getValue() instanceof String) {
                qmetric.addTag(String.valueOf(tag.getKey()), (String) tag.getValue());
                groupby.add((String) tag.getValue());
              } else if (tag.getValue() instanceof List) {
                qmetric.addTag(String.valueOf(tag.getKey()), ((List<String>) tag.getValue()).toArray(new String[0]));
                for (String elt: (List<String>) tag.getValue()) {
                  groupby.add(elt);
                }
              } else {
                throw new WarpScriptException(getName() + " expects tag names to be associated with a STRING or a list thereof.");
              }
            }
            
            if (!groupby.isEmpty()) {
              qmetric.addGrouper(new TagGrouper(new ArrayList<String>(groupby)));
            }
            
            if (!aggregators.isEmpty()) {
              for (Aggregator aggregator: aggregators) {
                qmetric.addAggregator(aggregator);
              }
            }
          }
        }
        
        QueryResponse response = client.query(builder);
        
        List<GeoTimeSerie> series = new ArrayList<GeoTimeSerie>();
        
        for (QueryResult qresult: response.getQueries()) {
          for (Result result: qresult.getResults()) {
            List<DataPoint> points = result.getDataPoints();
            GeoTimeSerie gts = new GeoTimeSerie(points.size());
            gts.setName(result.getName());
            for (Entry<String,List<String>> entry: result.getTags().entrySet()) {
              if (1 == entry.getValue().size()) {
                gts.setLabel(entry.getKey(), entry.getValue().get(0));
              } else {
                StringBuilder sb = new StringBuilder();
                for (String tagval: entry.getValue()) {
                  if (sb.length() > 0) {
                    sb.append(",");
                  }
                  sb.append(tagval);
                }
                gts.setLabel(entry.getKey(), sb.toString());
                gts.getMetadata().putToAttributes(entry.getKey(), "multi");
              }
            }
            
            for (DataPoint point: points) {
              long ts = point.getTimestamp() * Constants.TIME_UNITS_PER_MS;

              Object value = point.getValue();
              if (value instanceof LazilyParsedNumber) {
                LazilyParsedNumber lpn = (LazilyParsedNumber) value;
                if (lpn.toString().contains(".")) {
                  GTSHelper.setValue(gts, ts, point.doubleValue());                                                  
                } else {
                  GTSHelper.setValue(gts, ts, point.longValue());                                                                    
                }
              } else {
                GTSHelper.setValue(gts, ts, point.stringValue()); 
              }
            }
            
            series.add(gts);
          }
        }
        
        stack.push(series);
      }      
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " encountered an error while contacting the KairosDB instance.", e);
    }
    
    return stack;
  }
}
