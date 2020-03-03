/*
 * Copyright (C) 2016 Orange
 *
 * This software is distributed under the terms and conditions of the 'BSD-3-Clause'
 * license which can be found in the file 'LICENSE.txt' in this package distribution
 * or at 'https://opensource.org/licenses/BSD-3-Clause'.
 */
package com.orange.liveobjects.samples.utils;

import java.util.Map;

public class DeviceCommandResponse {

    public DeviceCommandResponse() {
    }

    public DeviceCommandResponse(Map<String, Object> res, Long cid) {
        this.res = res;
        this.cid = cid;
    }

    public Map<String, Object> res;
    public Long cid;

}
