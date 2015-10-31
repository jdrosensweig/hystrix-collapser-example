package com.xyleolabs.test.collapser;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.xyleolabs.test.collapser.ExampleCollapserConstants.EXAMPLE_GROUP;

/**
 * Created by J.D. Rosensweig on 8/22/15.
 */
public class ExampleBatchCmd extends HystrixCommand<List<Boolean>> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private List<Integer> numbers;
    private ArrayList<Boolean> response = new ArrayList<>();
    private String threadName;

    public ExampleBatchCmd(List<Integer> numbers) {
        super(Setter.withGroupKey(
                HystrixCommandGroupKey.Factory.asKey(EXAMPLE_GROUP)));
        this.numbers = numbers;
    }

    @Override
    protected List<Boolean> run() throws Exception {
        threadName = Thread.currentThread().getName();
        addNumbersAndOutput();
        simulateStaticNetworkTime();
        setAllResponsesTo(true);
        LOG.debug("Finished summing {} numbers", numbers.size());
        return response;
    }

    private void addNumbersAndOutput() {
        final int sum = numbers.stream().reduce(0, (x, y) -> x + y);
        LOG.debug("Summed {} numbers and got {}", numbers.size(), sum);
    }

    private void simulateStaticNetworkTime() throws Exception {
        Thread.sleep(50);
    }

    private void setAllResponsesTo(boolean result) {
        numbers.stream().forEach(number -> response.add(result));
    }

    @Override
    protected List<Boolean> getFallback() {
        if (isFailedExecution()) {
            Throwable exception = getFailedExecutionException();
            LOG.warn("[{}] Exception = {}", threadName, exception.getMessage());
        } else {
            LOG.warn("[{}] Failure due to {}", threadName, getFallbackReason());
        }

        setAllResponsesTo(false);
        return response;
    }

    protected String getFallbackReason() {
        if (isCircuitBreakerOpen()) {
            return "Circuit Breaker Open";
        }

        if (isResponseRejected()) {
            return "Response rejected";
        }

        if (isResponseTimedOut()) {
            return "Response timed-out";
        }

        return "Unknown";
    }
}
