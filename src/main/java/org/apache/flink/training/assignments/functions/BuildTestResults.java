package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.training.assignments.domain.ComplianceResult;
import org.apache.flink.training.assignments.domain.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildTestResults implements MapFunction<Position, ComplianceResult> {
    private static final Logger LOG = LoggerFactory.getLogger(BuildTestResults.class);
    @Override
    public ComplianceResult map(Position p) throws Exception {
        //Log.debug("start of buildTestResults: {}",p.toString());
        ComplianceResult r = new ComplianceResult();
        r.setOrderId(p.getOrderId());
        r.setSuccess(true);
        r.setTimestamp(p.getTimestamp());
        return r;
    }
}
