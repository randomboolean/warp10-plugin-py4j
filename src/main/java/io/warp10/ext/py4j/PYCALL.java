//
//   Copyright 2021  SenX S.A.S.
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

package io.warp10.ext.py4j;

import io.warp10.plugins.py4j.Py4JWarp10Plugin;
import io.warp10.plugins.py4j.Py4JWarp10Plugin.CallbackExecutor;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.AbstractWarp10Plugin;
import io.warp10.warp.sdk.Capabilities;

public class PYCALL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    private static final String ATTRIBUTE_CLIENT_PREFIX = "py4j.callback.client";

    public PYCALL(String name) {
        super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {

        Object o = stack.pop();
        if (!(o instanceof String)) {
            throw new WarpScriptException(getName() + " expects a STRING.");
        }

        String execs = Capabilities.get(stack, PythonCallExtension.PY4J_EXECS);
        if (null == execs) {
            throw new WarpScriptException(getName() + " requires capability " + PythonCallExtension.PY4J_EXECS + ".");
        }

        int n = Integer.parseInt(execs);

        // TODO: handle concurrent PYCALL
        Integer executed;
        if (null == stack.getAttribute(PythonCallExtension.PY4J_EXECS)) {
            executed = 0;
        } else {
            executed = (Integer) stack.getAttribute(PythonCallExtension.PY4J_EXECS);
        }
        if (executed >= n) {
            throw new WarpScriptException(getName() + " have been called " + executed + " times, but token's capability" + PythonCallExtension.PY4J_EXECS + "is " + n +  ".");
        }
        stack.setAttribute(PythonCallExtension.PY4J_EXECS, executed + 1);

        if (null == Py4JWarp10Plugin.getGatewayServer()) {
            throw new WarpScriptException("GatewayServer is null. DEBUG plugins are:" + AbstractWarp10Plugin.plugins().get(0));
        }
        CallbackExecutor cbe = (CallbackExecutor) Py4JWarp10Plugin.getGatewayServer().getPythonServerEntryPoint(new Class[] {CallbackExecutor.class});

        try {
            String output = cbe.execute((String) o);
            stack.push(output);
        } catch (Exception e) {
            e.printStackTrace();
            throw new WarpScriptException(e.getCause());
        }

        return stack;
    }
}
