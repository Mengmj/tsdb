package com.alibaba.lindorm.contest.test;

import com.alibaba.lindorm.contest.custom.InternalSchema;
import com.alibaba.lindorm.contest.custom.RowWritable;
import com.alibaba.lindorm.contest.structs.Row;

import static com.alibaba.lindorm.contest.test.TestUtils.*;

public class SchemaTest {
    public static void main(String[] args) {
        InternalSchema schema = InternalSchema.build(TestUtils.TEST_SCHEMA);
        Row row = TestUtils.randomRow(TestUtils.TEST_SCHEMA);
    }
}
