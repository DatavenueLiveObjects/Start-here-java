/*
 * Copyright (C) 2016 Orange
 *
 * This software is distributed under the terms and conditions of the 'BSD-3-Clause'
 * license which can be found in the file 'LICENSE.txt' in this package distribution
 * or at 'https://opensource.org/licenses/BSD-3-Clause'.
 */
package com.orange.liveobjects.samples.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Structure of a "configuration message" that can be sent by a device into Live Objects.
 */
public class DeviceConfig {

    /**
     * Configuration parameter
     */
    public static class CfgParameter {
        public CfgParameter(String t, Object v) {
            this.t = t;
            this.v = v;
        }
        /**
         * Configuration parameter type ("str", "bin", "f64", "u32" or "i32")
         */
        public String t;

        /**
         * Configuration parameter value
         */
        public Object v;
    }

    /**
     * current device configuration
     */
    public final Map<String, CfgParameter> cfg = new HashMap<String, CfgParameter>();

}
