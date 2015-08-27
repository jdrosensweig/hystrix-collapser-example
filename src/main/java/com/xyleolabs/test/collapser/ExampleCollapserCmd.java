package com.xyleolabs.test.collapser;

import com.netflix.hystrix.HystrixCollapser;
import com.netflix.hystrix.HystrixCollapserKey;
import com.netflix.hystrix.HystrixCommand;
import com.sun.org.apache.bcel.internal.ExceptionConstants;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.xyleolabs.test.collapser.ExampleCollapserConstants.EXAMPLE_COLLAPSER_KEY;

/**
 * Created by J.D. Rosensweig on 8/22/15.
 */
public class ExampleCollapserCmd extends
        HystrixCollapser<List<Boolean>, Boolean, Integer> {
    private final Integer count;

    public ExampleCollapserCmd(Integer count) {
        super(Setter.withCollapserKey(
                HystrixCollapserKey.Factory.asKey(
                        EXAMPLE_COLLAPSER_KEY)));
        this.count = count;
    }

    @Override
    public Integer getRequestArgument() {
        return count;
    }

    @Override
    protected HystrixCommand<List<Boolean>> createCommand(
            Collection<CollapsedRequest<Boolean, Integer>> requests) {
        List<Integer> integerList = new ArrayList<>();
        for (CollapsedRequest<Boolean, Integer> r : requests) {
            integerList.add(r.getArgument());
        }

        return new ExampleBatchCmd(integerList);
    }

    @Override
    protected void mapResponseToRequests(
            List<Boolean> batchResponse,
            Collection<CollapsedRequest<Boolean, Integer>> requests) {
        int count = 0;
        for (CollapsedRequest<Boolean, Integer> request : requests) {
            request.setResponse(batchResponse.get(count++));
        }
    }
}
