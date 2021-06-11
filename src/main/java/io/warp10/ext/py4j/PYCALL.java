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

import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;
import io.warp10.warp.sdk.Capabilities;
import py4j.CallbackClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class PYCALL extends NamedWarpScriptFunction implements WarpScriptStackFunction {

    private static final String ATTRIBUTE_CLIENT_PREFIX = "py4j.callback.client";

    public PYCALL(String name) {
        super(name);
    }

    @Override
    public Object apply(WarpScriptStack stack) throws WarpScriptException {

        String host = Capabilities.get(stack, PythonCallExtension.PY4J_PYTHON_HOST);
        if (null == Capabilities.get(stack, PythonCallExtension.PY4J_PYTHON_HOST)) {
            throw new WarpScriptException(getName() + " requires capability " + PythonCallExtension.PY4J_PYTHON_HOST + ".");
        }

        String port = Capabilities.get(stack, PythonCallExtension.PY4J_PYTHON_PORT);
        if (null == port) {
            throw new WarpScriptException(getName() + " requires capability " + PythonCallExtension.PY4J_PYTHON_PORT + ".");
        }

        CallbackClient cb;
        try {

            Object cbo = stack.getAttribute(ATTRIBUTE_CLIENT_PREFIX + "@" + host + ":" + port);
            if (null == cbo) {
                InetAddress pyaddr = InetAddress.getByName(host);
                int pyport = Integer.parseInt(port);

                cb = new CallbackClient(pyport, pyaddr);
            } else {

                //
                // A stack will reuse a callback client with same host:port if it has already created one
                //
                cb = (CallbackClient) cbo;
            }

        } catch (UnknownHostException uhe) {
            throw new WarpScriptException(uhe.getCause());
        }

        Object o = stack.pop();
        if (!(o instanceof String)) {
            throw new WarpScriptException(getName() + " expects a STRING command.");
        }

        String response = cb.sendCommand((String) o);
        stack.push(response);

        return stack;
    }
}
