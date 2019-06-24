/** 
* Copyright (C) 2012-2017 ZEEI Inc.All Rights Reserved.
* 项目名称：aas
* 文件名称：AlarmIDUtil.java
* 包  名  称：com.zeei.das.aas.alarm
* 文件描述：TODO 请修改文件描述
* 创建日期：2017年6月16日上午11:41:07
* 
* 修改历史
* 1.0 quanhongsheng 2017年6月16日上午11:41:07 创建文件
*
*/

package com.zeei.das.aas.alarm;

import com.zeei.das.common.constants.DataType;
import com.zeei.das.common.utils.StringUtil;

/**
 * 类 名 称：AlarmIDUtil 类 描 述：TODO 请修改文件描述 功能描述：TODO 请修改功能描述 创建作者：quanhongsheng
 */

public class AlarmIDUtil {

	public static String generatingAlarmID(String pointCode, String alarmCode, String polluteCode, String dataType) {

		if (StringUtil.isEmptyOrNull(pointCode)) {
			pointCode = "-1";
		}

		if (StringUtil.isEmptyOrNull(alarmCode)) {
			alarmCode = "-1";
		}

		if (StringUtil.isEmptyOrNull(polluteCode)) {
			polluteCode = "-1";
		}

		if (StringUtil.isEmptyOrNull(dataType)) {
			dataType = DataType.T2011;
		}

		// 根据站点ID，告警码和因子ID及数据类型，取md5 作为规则的ID
		String str = String.format("%s_%s_%s_%s", pointCode, alarmCode, polluteCode, dataType);

		//String alarmId = Md5Util.getMd5(str);

		return str;

	}
	
	public static void main(String[] agrs){
		String s=generatingAlarmID("10","10001",null,null);
	}
}
