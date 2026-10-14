package io.kestra.controller.grpc.services;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import io.kestra.core.exceptions.DeserializationException;
import io.kestra.core.lock.LockService;
import io.kestra.core.queues.KeyedDispatchQueueInterface;
import io.kestra.core.queues.QueueException;
import io.kestra.core.queues.QueueSubscriber;
import io.kestra.core.runners.WorkerJobEvent;
import io.kestra.core.runners.WorkerJobEvent.StickyRouting;
import io.kestra.core.server.ServiceLivenessStore;
import io.kestra.core.utils.Disposable;
import io.kestra.core.utils.Either;
import io.kestra.core.vnodes.ControllerVNodesAssignmentChannel;
import io.kestra.core.vnodes.ControllerVNodesConfiguration;
import io.kestra.core.vnodes.VNodeController;
import io.kestra.core.vnodes.VNodesAssigner;
import io.kestra.core.vnodes.internals.DefaultVNodesAssigner;
import io.kestra.core.worker.Controller;
import io.kestra.core.worker.StickyWorkerRouting;

import io.micronaut.context.BeanProvider;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Takes part in the vNodes assignment of the worker controllers, subscribes this controller to the vNodes it is
 * assigned, and dispatches the sticky jobs received on them, forwarding the ones whose worker is held by another
 * controller.
 */
@Singleton
@Slf4j
@Requires(beans = ServiceLivenessStore.class)
public class StickyJobVNodeSubscriber implements WorkerLifecycleListener {

    private static final Duration REBALANCE_CHECK_INTERVAL = Duration.ofSeconds(5);

    private final KeyedDispatchQueueInterface<WorkerJobEvent> workerJobEventQueue;
    private final StickyWorkerRouting routing;
    private final BeanProvider<Controller> controller;
    private final VNodeController vNodeController;
    private final DefaultVNodesAssigner vNodesAssigner;

    private final ScheduledExecutorService rebalancer = Executors.newSingleThreadScheduledExecutor(
        Thread.ofVirtual().name("sticky-vnodes-rebalancer").factory()
    );
    private final Map<Integer, QueueSubscriber<WorkerJobEvent>> subscribers = new ConcurrentHashMap<>();

    private volatile WorkerJobDispatcher dispatcher;
    private volatile Disposable assignment;

    @Inject
    public StickyJobVNodeSubscriber(KeyedDispatchQueueInterface<WorkerJobEvent> workerJobEventQueue,
        StickyWorkerRouting routing,
        BeanProvider<Controller> controller,
        ControllerVNodesAssignmentChannel channel,
        ControllerVNodesConfiguration configuration,
        LockService lockService,
        ServiceLivenessStore serviceLivenessStore) {
        this.workerJobEventQueue = workerJobEventQueue;
        this.routing = routing;
        this.controller = controller;
        this.vNodeController = new VNodeController(lockService, serviceLivenessStore, channel, configuration.count(), configuration.rebalanceTimeout());
        this.vNodesAssigner = new DefaultVNodesAssigner(channel);
    }

    @Override
    public void init(WorkerJobDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        vNodesAssigner.start();
        rebalancer.scheduleWithFixedDelay(this::checkRebalance, 0, REBALANCE_CHECK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * @return the vNodes this controller is assigned, published with its liveness.
     */
    public Set<Integer> assignedVNodes() {
        return Set.copyOf(subscribers.keySet());
    }

    void checkRebalance() {
        try {
            // the controller service, whose id identifies this controller in the assignment, is built after the dispatcher
            if (assignment == null) {
                assignment = vNodesAssigner.subscribe(controller.get().getId(), new VNodesAssigner.VNodeAssignmentListener() {
                    @Override
                    public void onVNodesRevoked() {
                        subscribers.keySet().stream().toList().forEach(vNode -> closeQuietly(vNode, subscribers.remove(vNode)));
                    }

                    @Override
                    public void onVNodesAssigned(Set<Integer> vNodes) {
                        vNodes.forEach(vNode -> subscribers.computeIfAbsent(vNode, StickyJobVNodeSubscriber.this::subscribe));
                    }
                });
            }
            vNodeController.checkServicesAndRebalanceVNodes();
        } catch (Exception e) {
            log.warn("Failed to check the vNodes assignment of the worker controllers: {}", e.getMessage(), e);
        }
    }

    private QueueSubscriber<WorkerJobEvent> subscribe(int vNode) {
        QueueSubscriber<WorkerJobEvent> subscriber = workerJobEventQueue.subscriber(StickyWorkerRouting.routingKey(vNode));
        subscriber.subscribe(this::handle);
        log.info("Subscribed to vNode {} for sticky jobs", vNode);
        return subscriber;
    }

    private void handle(Either<WorkerJobEvent, DeserializationException> either) {
        if (either.isRight()) {
            log.error("Deserialization error for a sticky job: {}", either.getRight().getMessage());
            return;
        }
        WorkerJobEvent event = either.getLeft();
        if (!dispatcher.dispatchSticky(event)) {
            forward(event);
        }
    }

    private void forward(WorkerJobEvent event) {
        StickyRouting stickyRouting = event.stickyRouting();
        try {
            Optional<String> routingKey = routing.routingKeyForWorker(stickyRouting.workerId());
            if (routingKey.isPresent()) {
                WorkerJobEvent forwarded = event.withStickyRouting(stickyRouting.withVNodeRoutingKey(routingKey.get()));
                workerJobEventQueue.emit(forwarded.key(), forwarded);
            } else if (routing.isWorkerLost(stickyRouting.workerId())) {
                log.warn(
                    "Dropping the job {}: its worker '{}' is lost, so the liveness coordinator recovers it.",
                    event.job().uid(), stickyRouting.workerId()
                );
            } else {
                workerJobEventQueue.emit(event.key(), event);
            }
        } catch (QueueException e) {
            log.error("Failed to forward the sticky job {}: {}", event.job().uid(), e.getMessage(), e);
        }
    }

    private void closeQuietly(int vNode, QueueSubscriber<WorkerJobEvent> subscriber) {
        try {
            subscriber.close();
            log.info("Unsubscribed from vNode {} for sticky jobs", vNode);
        } catch (Exception e) {
            log.warn("Error closing the subscription to vNode {}: {}", vNode, e.getMessage());
        }
    }

    @PreDestroy
    public void close() {
        rebalancer.shutdownNow();
        if (assignment != null) {
            assignment.dispose();
        }
        vNodesAssigner.stop();
        vNodeController.close();
        subscribers.forEach(this::closeQuietly);
        subscribers.clear();
    }
}
