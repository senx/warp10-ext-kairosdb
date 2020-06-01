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

import io.warp10.WarpConfig;
import io.warp10.script.MemoryWarpScriptStack;
import io.warp10.script.WarpScriptException;

public class URLValidator {
  
  private static final String validator;
  
  static {
    validator = WarpConfig.getProperty(KairosDBWarpScriptExtension.CONFIG_VALIDATOR, "");
  }
  
  public static String validate(String url) throws WarpScriptException {
    MemoryWarpScriptStack stack = new MemoryWarpScriptStack(null, null);
    stack.push(url);
    stack.exec(validator);
    return String.valueOf(stack.pop());
  }
}
