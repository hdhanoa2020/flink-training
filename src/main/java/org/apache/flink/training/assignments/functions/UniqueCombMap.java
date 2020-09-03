package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.training.assignments.domain.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class UniqueCombMap implements MapFunction<Position, Set<String>>  {

    private static final Logger LOG = LoggerFactory.getLogger(OrderCusipMap.class);

    @Override
    public Set<String> map(Position position) throws Exception {
            Set<String> s = new HashSet<String>();
                s.add(position.getCusip()+position.getAccount()+position.getSubAccount());
                return s;
            }
}
