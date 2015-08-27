package com.xyleolabs.test.collapser;

import com.netflix.hystrix.Hystrix;
import com.netflix.hystrix.HystrixCollapser;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.HystrixThreadPoolMetrics;
import com.netflix.hystrix.strategy.concurrency.HystrixRequestContext;
import com.netflix.config.ConfigurationManager;
import org.apache.commons.configuration.AbstractConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observer;

import java.util.concurrent.atomic.AtomicInteger;

import static com.xyleolabs.test.collapser.ExampleCollapserConstants.EXAMPLE_GROUP;

/**
 * Created by J.D. Rosensweig on 8/22/15.
 */
public class ExampleCollapserMain {
    private static Logger LOG, CLOG;

    private static AbstractConfiguration hystrixConfig;
    private static final String CORE_SIZE_CONFIG =
            "hystrix.threadpool.default.coreSize";
    private static final String MAX_QUEUE_SIZE_CONFIG =
            "hystrix.threadpool.default.maxQueueSize";
    private static final String QUEUE_REJECTION_THRESHOLD_CONFIG =
            "hystrix.threadpool.default.queueSizeRejectionThreshold";
    private static final String COLLAPSER_TIMER_DELAY =
            "hystrix.collapser.default.timerDelayInMilliseconds";

    private static AtomicInteger commandErrors = new AtomicInteger(0);
    private static AtomicInteger commandSuccesses = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        LOG = LoggerFactory.getLogger(ExampleCollapserMain.class);
        CLOG = LoggerFactory.getLogger("console");
        initializeHystrixSettings();

        HystrixRequestContext context =
                HystrixRequestContext.initializeContext();
        try {
            runLoopTest();
        } finally {
            context.shutdown();
        }
    }

    private static void initializeHystrixSettings() {
        hystrixConfig = ConfigurationManager.getConfigInstance();
        hystrixConfig.setProperty(CORE_SIZE_CONFIG, "50");
        hystrixConfig.setProperty(MAX_QUEUE_SIZE_CONFIG, "50");
        hystrixConfig.setProperty(QUEUE_REJECTION_THRESHOLD_CONFIG, "25");
        hystrixConfig.setProperty(COLLAPSER_TIMER_DELAY, "30");
    }

    private static void runLoopTest() throws InterruptedException {
        final int commandsToRun = 5000;
        for (int number = 0; number < commandsToRun; number++) {
            try {
                HystrixCollapser collapser = new ExampleCollapserCmd(number);
                Observable observable = collapser.toObservable();
                observable.subscribe(new Observer<Boolean>() {
                    @Override
                    public void onCompleted() {
                    }

                    @Override
                    public void onError(Throwable e) {
                        commandErrors.incrementAndGet();
                    }

                    @Override
                    public void onNext(Boolean result) {
                        if (result) {
                            commandSuccesses.incrementAndGet();
                        } else {
                            commandErrors.incrementAndGet();
                        }
                    }
                });
            } catch (Exception e) {
                LOG.debug("Exception = ", e);
            }

            Thread.sleep(1);
        }

        while (getTotalWaitingCommands() > 0) {
            CLOG.info("Waiting for Hystrix pool to finish all jobs");
            Thread.sleep(50);
        }

        CLOG.info("Successful command runs = {}", commandSuccesses.get());
        CLOG.info("Failure command runs = {}", commandErrors.get());

        // For good measure.  Occassionally hystrix threads won't clean up.
        Hystrix.reset();
    }

    private static int getTotalWaitingCommands() {
        int totalWaitingCommands = 0;
        final String hystrixPoolName = EXAMPLE_GROUP;

        HystrixThreadPoolKey key =
                HystrixThreadPoolKey.Factory.asKey(hystrixPoolName);

        HystrixThreadPoolMetrics metrics = HystrixThreadPoolMetrics.getInstance(key);

        if (metrics != null) {
            Number active = metrics.getCurrentActiveCount();
            Number queueSize = metrics.getCurrentQueueSize();
            totalWaitingCommands = active.intValue() + queueSize.intValue();
            LOG.debug("{} has {} commands waiting", hystrixPoolName, totalWaitingCommands);
        }

        return totalWaitingCommands;
    }
}
