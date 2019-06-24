/** 
* Copyright (C) 2012-2017 ZEEI Inc.All Rights Reserved.
* 项目名称：aas
* 文件名称：ParamUtil.java
* 包  名  称：com.zeei.das.aas.alarm
* 文件描述：TODO 请修改文件描述
* 创建日期：2017年4月21日下午3:12:42
* 
* 修改历史
* 1.0 quanhongsheng 2017年4月21日下午3:12:42 创建文件
*
*/

package com.zeei.das.aas.alarm;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 类 名 称：ParamUtil 类 描 述：TODO 请修改文件描述 功能描述：TODO 请修改功能描述 创建作者：quanhongsheng
 */

public class ParamUtil {

	/**
	 * 根据因子id，查询因子对象 findParam:TODO 请修改方法功能描述
	 *
	 * @param paramId
	 *            因子ID
	 * @param data
	 *            数据集合
	 * @return Map<String,String>
	 */
	public static JSONObject findParam(String paramId, JSONArray data) {

		JSONObject retMap = null;

		for (int i = 0; i < data.size(); i++) {
			JSONObject map = data.getJSONObject(i);
			if (map != null && paramId.equals(map.get("ParamID"))) {
				retMap = map;
				break;
			}
		}
		return retMap;
	}

}
