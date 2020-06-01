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

import java.util.ArrayList;
import java.util.List;

import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Metric;
import org.kairosdb.client.builder.MetricBuilder;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

public class KUPDATE extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public KUPDATE(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    
    Object top = stack.pop();
    
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a KairosDB endpoint URL.");
    }
    
    String url = URLValidator.validate((String) top);
    
    top = stack.pop();
    
    List<Object> inputs = null;
    if (top instanceof GeoTimeSerie || top instanceof GTSEncoder) {
      inputs = new ArrayList<Object>(1);
      inputs.add(top);
    } else if (top instanceof List) {
      inputs = (List<Object>) top;
    } else {
      throw new WarpScriptException(getName() + " operates on a Geo Time Series, a GTS Encoder or a list of such elements.");
    }
    try {
      try(HttpClient client = new HttpClient(url)) {
        MetricBuilder builder = MetricBuilder.getInstance();
        for (Object input: inputs) {
          if (input instanceof GTSEncoder) {
            GTSDecoder decoder = ((GTSEncoder) input).getDecoder(true);
            Metric metric = builder.addMetric(decoder.getName());
            metric.addTags(decoder.getLabels());
            while(decoder.next()) {
              // Convert timestamp to ms
              long ts = decoder.getTimestamp() / Constants.TIME_UNITS_PER_MS;
              metric.addDataPoint(ts, decoder.getValue());
            }
          } else if (input instanceof GeoTimeSerie) {
            GeoTimeSerie gts = (GeoTimeSerie) input;
            Metric metric = builder.addMetric(gts.getName());
            metric.addTags(gts.getLabels());
            int n = GTSHelper.nvalues(gts);
            for (int i = 0; i < n; i++) {
              long ts = GTSHelper.tickAtIndex(gts, i) / Constants.TIME_UNITS_PER_MS;
              metric.addDataPoint(ts, GTSHelper.valueAtIndex(gts, i));
            }
          } else {
            throw new WarpScriptException(getName() + " operates on a Geo Time Series, a GTS Encoder or a list of such elements.");
          }
        }
        client.pushMetrics(builder);
      }      
    } catch (Exception e) {
      throw new WarpScriptException(getName() + " encountered an error while connecting to KairosDB instance.", e);
    }
    
    return stack;
  }
}
