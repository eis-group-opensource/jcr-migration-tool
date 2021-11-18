/* Copyright © 2016 EIS Group and/or one of its affiliates. All rights reserved. Unpublished work under U.S. copyright laws.
 CONFIDENTIAL AND TRADE SECRET INFORMATION. No portion of this work may be copied, distributed, modified, or incorporated into any other media without EIS Group prior written consent.*/
package com.exigen.eis.jcr;

import static org.junit.Assert.*;

import org.junit.Test;

public class ExportTest {

	@Test
	public void testParseEntityType() {
		String result = Export.parseEntityType("/397:efolder{1}/BillingAccount{1}/allTypes{1}/BILL_STMT{1}");
		assertEquals("BillingAccount", result);
	}

}
