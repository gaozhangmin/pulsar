/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.broker.admin.impl;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.common.policies.data.PoliciesUtil.defaultBundle;
import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang.mutable.MutableObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.admin.AdminResource;
import org.apache.pulsar.broker.authorization.AuthorizationService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;
import org.apache.pulsar.common.naming.NamedEntity;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoSubscriptionCreationOverride;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.EntryFilters;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.Policies.BundleType;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.SubscriptionAuthMode;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicHashPositions;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.ValidateResult;
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class NamespacesBase extends AdminResource {

    protected CompletableFuture<List<String>> internalGetTenantNamespaces(String tenant) {
        if (tenant == null) {
            return FutureUtil.failedFuture(new RestException(Status.BAD_REQUEST, "Tenant should not be null"));
        }
        try {
            NamedEntity.checkName(tenant);
        } catch (IllegalArgumentException e) {
            log.warn("[{}] Tenant name is invalid {}", clientAppId(), tenant, e);
            return FutureUtil.failedFuture(new RestException(Status.PRECONDITION_FAILED, "Tenant name is not valid"));
        }
        return validateTenantOperationAsync(tenant, TenantOperation.LIST_NAMESPACES)
                .thenCompose(__ -> tenantResources().tenantExistsAsync(tenant))
                .thenCompose(existed -> {
                    if (!existed) {
                        throw new RestException(Status.NOT_FOUND, "Tenant not found");
                    }
                    return tenantResources().getListOfNamespacesAsync(tenant);
                });
    }

    protected CompletableFuture<Void> internalCreateNamespace(Policies policies) {
        return validateTenantOperationAsync(namespaceName.getTenant(), TenantOperation.CREATE_NAMESPACE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> validatePoliciesAsync(namespaceName, policies))
                .thenCompose(__ -> {
                    int maxNamespacesPerTenant = pulsar().getConfiguration().getMaxNamespacesPerTenant();
                    // no distributed locks are added here.In a concurrent scenario, the threshold will be exceeded.
                    if (maxNamespacesPerTenant > 0) {
                        return tenantResources().getListOfNamespacesAsync(namespaceName.getTenant())
                                .thenAccept(namespaces -> {
                                    if (namespaces != null && namespaces.size() > maxNamespacesPerTenant) {
                                        throw new RestException(Status.PRECONDITION_FAILED,
                                                "Exceed the maximum number of namespace in tenant :"
                                                        + namespaceName.getTenant());
                                    }
                                });
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .thenCompose(__ -> namespaceResources().createPoliciesAsync(namespaceName, policies))
                .thenAccept(__ -> log.info("[{}] Created namespace {}", clientAppId(), namespaceName));
    }

    protected void internalDeleteNamespace(AsyncResponse asyncResponse, boolean authoritative, boolean force) {
        if (force) {
            internalDeleteNamespaceForcefully(asyncResponse, authoritative);
        } else {
            internalDeleteNamespace(asyncResponse, authoritative);
        }
    }

    protected CompletableFuture<List<String>> internalGetListOfTopics(Policies policies,
                                                                      CommandGetTopicsOfNamespace.Mode mode) {
        switch (mode) {
            case ALL:
                return pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName)
                        .thenCombine(internalGetNonPersistentTopics(policies),
                                (persistentTopics, nonPersistentTopics) ->
                                        ListUtils.union(persistentTopics, nonPersistentTopics));
            case NON_PERSISTENT:
                return internalGetNonPersistentTopics(policies);
            case PERSISTENT:
            default:
                return pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName);
        }
    }

    protected CompletableFuture<List<String>> internalGetNonPersistentTopics(Policies policies) {
        final List<CompletableFuture<List<String>>> futures = Lists.newArrayList();
        final List<String> boundaries = policies.bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            final String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            try {
                futures.add(pulsar().getAdminClient().topics()
                        .getListInBundleAsync(namespaceName.toString(), bundle));
            } catch (PulsarServerException e) {
                throw new RestException(e);
            }
        }
        return FutureUtil.waitForAll(futures)
                .thenApply(__ -> {
                    final List<String> topics = Lists.newArrayList();
                    for (int i = 0; i < futures.size(); i++) {
                        List<String> topicList = futures.get(i).join();
                        if (topicList != null) {
                            topics.addAll(topicList);
                        }
                    }
                    return topics.stream().filter(name -> !TopicName.get(name).isPersistent())
                            .collect(Collectors.toList());
                });
    }

    @SuppressWarnings("deprecation")
    protected void internalDeleteNamespace(AsyncResponse asyncResponse, boolean authoritative) {
        validateTenantOperation(namespaceName.getTenant(), TenantOperation.DELETE_NAMESPACE);
        validatePoliciesReadOnlyAccess();

        // ensure that non-global namespace is directed to the correct cluster
        if (!namespaceName.isGlobal()) {
            validateClusterOwnership(namespaceName.getCluster());
        }

        Policies policies = null;

        // ensure the local cluster is the only cluster for the global namespace configuration
        try {
            policies = namespaceResources().getPolicies(namespaceName).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist."));
            if (namespaceName.isGlobal()) {
                if (policies.replication_clusters.size() > 1) {
                    // There are still more than one clusters configured for the global namespace
                    throw new RestException(Status.PRECONDITION_FAILED, "Cannot delete the global namespace "
                            + namespaceName + ". There are still more than one replication clusters configured.");
                }
                if (policies.replication_clusters.size() == 1
                        && !policies.replication_clusters.contains(config().getClusterName())) {
                    // the only replication cluster is other cluster, redirect
                    String replCluster = Lists.newArrayList(policies.replication_clusters).get(0);
                    ClusterData replClusterData = clusterResources().getCluster(replCluster)
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                    "Cluster " + replCluster + " does not exist"));
                    URL replClusterUrl;
                    if (!config().isTlsEnabled() || !isRequestHttps()) {
                        replClusterUrl = new URL(replClusterData.getServiceUrl());
                    } else if (StringUtils.isNotBlank(replClusterData.getServiceUrlTls())) {
                        replClusterUrl = new URL(replClusterData.getServiceUrlTls());
                    } else {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "The replication cluster does not provide TLS encrypted service");
                    }
                    URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(replClusterUrl.getHost())
                            .port(replClusterUrl.getPort()).replaceQueryParam("authoritative", false).build();
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(), redirect,
                                replCluster);
                    }
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
            return;
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        boolean isEmpty;
        List<String> topics;
        try {
            topics = pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName)
                    .get(config().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
            topics.addAll(getPartitionedTopicList(TopicDomain.persistent));
            topics.addAll(getPartitionedTopicList(TopicDomain.non_persistent));
            isEmpty = topics.isEmpty();

        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        if (!isEmpty) {
            if (log.isDebugEnabled()) {
                log.debug("Found topics on namespace {}", namespaceName);
            }
            boolean hasNonSystemTopic = false;
            for (String topic : topics) {
                if (!pulsar().getBrokerService().isSystemTopic(TopicName.get(topic))) {
                    hasNonSystemTopic = true;
                    break;
                }
            }
            if (hasNonSystemTopic) {
                asyncResponse.resume(new RestException(Status.CONFLICT, "Cannot delete non empty namespace"));
                return;
            }
        }

        // set the policies to deleted so that somebody else cannot acquire this namespace
        try {
            namespaceResources().setPolicies(namespaceName, old -> {
                old.deleted = true;
                return old;
            });
        } catch (Exception e) {
            log.error("[{}] Failed to delete namespace on global ZK {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
            return;
        }

        // remove from owned namespace map and ephemeral node from ZK
        final List<CompletableFuture<Void>> futures = Lists.newArrayList();
        // remove system topics first.
        if (!topics.isEmpty()) {
            for (String topic : topics) {
                try {
                    futures.add(pulsar().getAdminClient().topics().deleteAsync(topic, true, true));
                } catch (Exception ex) {
                    log.error("[{}] Failed to delete system topic {}", clientAppId(), topic, ex);
                    asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, ex));
                    return;
                }
            }
        }
        FutureUtil.waitForAll(futures).thenCompose(__ -> {
            List<CompletableFuture<Void>> deleteBundleFutures = Lists.newArrayList();
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                            .getBundles(namespaceName);
            for (NamespaceBundle bundle : bundles.getBundles()) {
                        // check if the bundle is owned by any broker, if not then we do not need to delete the bundle
                deleteBundleFutures.add(pulsar().getNamespaceService().getOwnerAsync(bundle).thenCompose(ownership -> {
                    if (ownership.isPresent()) {
                        try {
                            return pulsar().getAdminClient().namespaces()
                                    .deleteNamespaceBundleAsync(namespaceName.toString(), bundle.getBundleRange());
                        } catch (PulsarServerException e) {
                            throw new RestException(e);
                        }
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                }));
            }
            return FutureUtil.waitForAll(deleteBundleFutures);
        })
        .thenCompose(__ -> internalClearZkSources())
        .thenAccept(__ -> {
            log.info("[{}] Remove namespace successfully {}", clientAppId(), namespaceName);
            asyncResponse.resume(Response.noContent().build());
        })
        .exceptionally(ex -> {
            Throwable cause = FutureUtil.unwrapCompletionException(ex);
            log.error("[{}] Failed to remove namespace {}", clientAppId(), namespaceName, cause);
            if (cause instanceof PulsarAdminException.ConflictException) {
                log.info("[{}] There are new topics created during the namespace deletion, "
                        + "retry to delete the namespace again.", namespaceName);
                pulsar().getExecutor().execute(() -> internalDeleteNamespace(asyncResponse, authoritative));
            } else {
                resumeAsyncResponseExceptionally(asyncResponse, ex);
            }
            return null;
        });
    }

    // clear zk-node resources for deleting namespace
    protected CompletableFuture<Void> internalClearZkSources() {
        // clear resource of `/namespace/{namespaceName}` for zk-node
        return namespaceResources().deleteNamespaceAsync(namespaceName)
                .thenCompose(ignore -> namespaceResources().getPartitionedTopicResources()
                        .clearPartitionedTopicMetadataAsync(namespaceName))
                // clear resource for manager-ledger z-node
                .thenCompose(ignore -> pulsar().getPulsarResources().getTopicResources()
                        .clearDomainPersistence(namespaceName))
                .thenCompose(ignore -> pulsar().getPulsarResources().getTopicResources()
                        .clearNamespacePersistence(namespaceName))
                // we have successfully removed all the ownership for the namespace, the policies
                // z-node can be deleted now
                .thenCompose(ignore -> namespaceResources().deletePoliciesAsync(namespaceName))
                // clear z-node of local policies
                .thenCompose(ignore -> getLocalPolicies().deleteLocalPoliciesAsync(namespaceName))
                // clear /loadbalance/bundle-data
                .thenCompose(ignore -> namespaceResources().deleteBundleDataAsync(namespaceName));

    }

    @SuppressWarnings("deprecation")
    protected void internalDeleteNamespaceForcefully(AsyncResponse asyncResponse, boolean authoritative) {
        validateTenantOperation(namespaceName.getTenant(), TenantOperation.DELETE_NAMESPACE);
        validatePoliciesReadOnlyAccess();

        if (!pulsar().getConfiguration().isForceDeleteNamespaceAllowed()) {
            asyncResponse.resume(
                    new RestException(Status.METHOD_NOT_ALLOWED, "Broker doesn't allow forced deletion of namespaces"));
            return;
        }

        // ensure that non-global namespace is directed to the correct cluster
        if (!namespaceName.isGlobal()) {
            validateClusterOwnership(namespaceName.getCluster());
        }

        Policies policies = null;

        // ensure the local cluster is the only cluster for the global namespace configuration
        try {
            policies = namespaceResources().getPolicies(namespaceName).orElseThrow(
                    () -> new RestException(Status.NOT_FOUND, "Namespace " + namespaceName + " does not exist."));
            if (namespaceName.isGlobal()) {
                if (policies.replication_clusters.size() > 1) {
                    // There are still more than one clusters configured for the global namespace
                    throw new RestException(Status.PRECONDITION_FAILED, "Cannot delete the global namespace "
                            + namespaceName + ". There are still more than one replication clusters configured.");
                }
                if (policies.replication_clusters.size() == 1
                        && !policies.replication_clusters.contains(config().getClusterName())) {
                    // the only replication cluster is other cluster, redirect
                    String replCluster = Lists.newArrayList(policies.replication_clusters).get(0);
                    ClusterData replClusterData =
                            clusterResources().getCluster(replCluster)
                            .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                                    "Cluster " + replCluster + " does not exist"));
                    URL replClusterUrl;
                    if (!config().isTlsEnabled() || !isRequestHttps()) {
                        replClusterUrl = new URL(replClusterData.getServiceUrl());
                    } else if (StringUtils.isNotBlank(replClusterData.getServiceUrlTls())) {
                        replClusterUrl = new URL(replClusterData.getServiceUrlTls());
                    } else {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "The replication cluster does not provide TLS encrypted service");
                    }
                    URI redirect = UriBuilder.fromUri(uri.getRequestUri()).host(replClusterUrl.getHost())
                            .port(replClusterUrl.getPort()).replaceQueryParam("authoritative", false).build();
                    if (log.isDebugEnabled()) {
                        log.debug("[{}] Redirecting the rest call to {}: cluster={}", clientAppId(), redirect,
                                replCluster);
                    }
                    throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                }
            }
        } catch (WebApplicationException wae) {
            asyncResponse.resume(wae);
            return;
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        List<String> topics;
        try {
            topics = pulsar().getNamespaceService().getFullListOfTopics(namespaceName)
                    .get(config().getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            asyncResponse.resume(new RestException(e));
            return;
        }

        // set the policies to deleted so that somebody else cannot acquire this namespace
        try {
            namespaceResources().setPolicies(namespaceName, old -> {
                old.deleted = true;
                return old;
            });
        } catch (Exception e) {
            log.error("[{}] Failed to delete namespace on global ZK {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
            return;
        }

        // remove from owned namespace map and ephemeral node from ZK
        final List<CompletableFuture<Void>> topicFutures = Lists.newArrayList();
        final List<CompletableFuture<Void>> bundleFutures = Lists.newArrayList();
        try {
            // firstly remove all topics including system topics
            if (!topics.isEmpty()) {
                Set<String> partitionedTopics = new HashSet<>();
                Set<String> nonPartitionedTopics = new HashSet<>();

                for (String topic : topics) {
                    try {
                        TopicName topicName = TopicName.get(topic);
                        if (topicName.isPartitioned()) {
                            String partitionedTopic = topicName.getPartitionedTopicName();
                            if (!partitionedTopics.contains(partitionedTopic)) {
                                partitionedTopics.add(partitionedTopic);
                            }
                        } else {
                            nonPartitionedTopics.add(topic);
                        }
                        topicFutures.add(pulsar().getAdminClient().topics().deleteAsync(topic, true));
                    } catch (Exception e) {
                        String errorMessage = String.format("Failed to force delete topic %s, "
                                        + "but the previous deletion command of partitioned-topics:%s "
                                        + "and non-partitioned-topics:%s have been sent out asynchronously. "
                                        + "Reason: %s",
                                topic, partitionedTopics, nonPartitionedTopics, e.getCause());
                        log.error("[{}] {}", clientAppId(), errorMessage, e);
                        asyncResponse.resume(new RestException(Status.INTERNAL_SERVER_ERROR, errorMessage));
                        return;
                    }
                }

                for (String partitionedTopic : partitionedTopics) {
                    topicFutures.add(namespaceResources().getPartitionedTopicResources()
                            .deletePartitionedTopicAsync(TopicName.get(partitionedTopic)));
                }

                if (log.isDebugEnabled()) {
                    log.debug("Successfully send deletion command of partitioned-topics:{} "
                                    + "and non-partitioned-topics:{} in namespace:{}.",
                            partitionedTopics, nonPartitionedTopics, namespaceName);
                }

                final CompletableFuture<Throwable> topicFutureEx =
                        FutureUtil.waitForAll(topicFutures).handle((result, exception) -> {
                            if (exception != null) {
                                if (exception.getCause() instanceof PulsarAdminException) {
                                    asyncResponse
                                            .resume(new RestException((PulsarAdminException) exception.getCause()));
                                } else {
                                    log.error("[{}] Failed to remove forcefully owned namespace {}",
                                            clientAppId(), namespaceName, exception);
                                    asyncResponse.resume(new RestException(exception.getCause()));
                                }
                                return exception;
                            }

                            return null;
                        });
                if (topicFutureEx.join() != null) {
                    return;
                }
            }

            // forcefully delete namespace bundles
            NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                    .getBundles(namespaceName);
            for (NamespaceBundle bundle : bundles.getBundles()) {
                // check if the bundle is owned by any broker, if not then we do not need to delete the bundle
                if (pulsar().getNamespaceService().getOwner(bundle).isPresent()) {
                    bundleFutures.add(pulsar().getAdminClient().namespaces()
                            .deleteNamespaceBundleAsync(namespaceName.toString(), bundle.getBundleRange(), true));
                }
            }
        } catch (Exception e) {
            log.error("[{}] Failed to remove forcefully owned namespace {}", clientAppId(), namespaceName, e);
            asyncResponse.resume(new RestException(e));
            return;
        }

        FutureUtil.waitForAll(bundleFutures).thenCompose(__ -> internalClearZkSources()).handle((result, exception) -> {
            if (exception != null) {
                Throwable cause = FutureUtil.unwrapCompletionException(exception);
                if (cause instanceof PulsarAdminException.ConflictException) {
                    log.info("[{}] There are new topics created during the namespace deletion, "
                            + "retry to force delete the namespace again.", namespaceName);
                    pulsar().getExecutor().execute(() ->
                            internalDeleteNamespaceForcefully(asyncResponse, authoritative));
                } else {
                    log.error("[{}] Failed to remove forcefully owned namespace {}",
                            clientAppId(), namespaceName, cause);
                    asyncResponse.resume(new RestException(cause));
                }
                return null;
            }
            asyncResponse.resume(Response.noContent().build());
            return null;
        });
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalDeleteNamespaceBundleAsync(String bundleRange, boolean authoritative,
                                                                         boolean force) {
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.DELETE_BUNDLE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    if (!namespaceName.isGlobal()) {
                        return validateClusterOwnershipAsync(namespaceName.getCluster());
                    }
                    return CompletableFuture.completedFuture(null);
                })
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies -> {
                    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
                    if (namespaceName.isGlobal()) {

                        if (policies.replication_clusters.size() > 1) {
                            // There are still more than one clusters configured for the global namespace
                            throw new RestException(Status.PRECONDITION_FAILED, "Cannot delete the global namespace "
                                    + namespaceName
                                    + ". There are still more than one replication clusters configured.");
                        }
                        if (policies.replication_clusters.size() == 1
                                && !policies.replication_clusters.contains(config().getClusterName())) {
                            // the only replication cluster is other cluster, redirect
                            String replCluster = Lists.newArrayList(policies.replication_clusters).get(0);
                            future = clusterResources().getClusterAsync(replCluster)
                                    .thenCompose(clusterData -> {
                                        if (clusterData.isEmpty()) {
                                            throw new RestException(Status.NOT_FOUND,
                                                    "Cluster " + replCluster + " does not exist");
                                        }
                                        ClusterData replClusterData = clusterData.get();
                                        URL replClusterUrl;
                                        try {
                                            if (!config().isTlsEnabled() || !isRequestHttps()) {
                                                replClusterUrl = new URL(replClusterData.getServiceUrl());
                                            } else if (StringUtils.isNotBlank(replClusterData.getServiceUrlTls())) {
                                                replClusterUrl = new URL(replClusterData.getServiceUrlTls());
                                            } else {
                                                throw new RestException(Status.PRECONDITION_FAILED,
                                                        "The replication cluster does not provide TLS encrypted "
                                                                + "service");
                                            }
                                        } catch (MalformedURLException malformedURLException) {
                                            throw new RestException(malformedURLException);
                                        }

                                        URI redirect =
                                                UriBuilder.fromUri(uri.getRequestUri()).host(replClusterUrl.getHost())
                                                        .port(replClusterUrl.getPort())
                                                        .replaceQueryParam("authoritative", false).build();
                                        if (log.isDebugEnabled()) {
                                            log.debug("[{}] Redirecting the rest call to {}: cluster={}",
                                                    clientAppId(), redirect, replCluster);
                                        }
                                        throw new WebApplicationException(Response.temporaryRedirect(redirect).build());
                                    });
                        }
                    }
                    return future.thenCompose(__ -> {
                        NamespaceBundle bundle =
                                validateNamespaceBundleOwnership(namespaceName, policies.bundles, bundleRange,
                                        authoritative, true);
                        return pulsar().getNamespaceService().getListOfPersistentTopics(namespaceName)
                                .thenCompose(topics -> {
                                    CompletableFuture<Void> deleteTopicsFuture =
                                            CompletableFuture.completedFuture(null);
                                    if (!force) {
                                        List<CompletableFuture<NamespaceBundle>> futures = new ArrayList<>();
                                        for (String topic : topics) {
                                            futures.add(pulsar().getNamespaceService()
                                                    .getBundleAsync(TopicName.get(topic))
                                                    .thenCompose(topicBundle -> {
                                                        if (bundle.equals(topicBundle)) {
                                                            throw new RestException(Status.CONFLICT,
                                                                    "Cannot delete non empty bundle");
                                                        }
                                                        return CompletableFuture.completedFuture(null);
                                                    }));

                                        }
                                        deleteTopicsFuture = FutureUtil.waitForAll(futures);
                                    }
                                    return deleteTopicsFuture.thenCompose(
                                            ___ -> pulsar().getNamespaceService().removeOwnedServiceUnitAsync(bundle))
                                            .thenRun(() -> pulsar().getBrokerService().getBundleStats()
                                                    .remove(bundle.toString()));
                                });
                    });
                });
    }

    protected CompletableFuture<Void> internalGrantPermissionOnNamespaceAsync(String role, Set<AuthAction> actions) {
        AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
        if (null != authService) {
            return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.GRANT_PERMISSION)
                    .thenAccept(__ -> {
                        checkNotNull(role, "Role should not be null");
                        checkNotNull(actions, "Actions should not be null");
                    }).thenCompose(__ ->
                            authService.grantPermissionAsync(namespaceName, actions, role, null))
                    .thenAccept(unused ->
                            log.info("[{}] Successfully granted access for role {}: {} - namespaceName {}",
                                    clientAppId(), role, actions, namespaceName))
                    .exceptionally(ex -> {
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        //The IllegalArgumentException and the IllegalStateException were historically thrown by the
                        // grantPermissionAsync method, so we catch them here to ensure backwards compatibility.
                        if (realCause instanceof MetadataStoreException.NotFoundException
                                || realCause instanceof IllegalArgumentException) {
                            log.warn("[{}] Failed to set permissions for namespace {}: does not exist", clientAppId(),
                                    namespaceName, ex);
                            throw new RestException(Status.NOT_FOUND, "Topic's namespace does not exist");
                        } else if (realCause instanceof MetadataStoreException.BadVersionException
                                || realCause instanceof IllegalStateException) {
                            log.warn("[{}] Failed to set permissions for namespace {}: {}",
                                    clientAppId(), namespaceName, ex.getCause().getMessage(), ex);
                            throw new RestException(Status.CONFLICT, "Concurrent modification");
                        } else {
                            log.error("[{}] Failed to get permissions for namespace {}",
                                    clientAppId(), namespaceName, ex);
                            throw new RestException(realCause);
                        }
                    });
        } else {
            String msg = "Authorization is not enabled";
            return FutureUtil.failedFuture(new RestException(Status.NOT_IMPLEMENTED, msg));
        }
    }


    protected CompletableFuture<Void> internalGrantPermissionOnSubscriptionAsync(String subscription,
                                                                                Set<String> roles) {
        AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
        if (null != authService) {
            return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.GRANT_PERMISSION)
                    .thenAccept(__ -> {
                        checkNotNull(subscription, "Subscription should not be null");
                        checkNotNull(roles, "Roles should not be null");
                    })
                    .thenCompose(__ -> authService.grantSubscriptionPermissionAsync(namespaceName, subscription,
                            roles, null))
                    .thenAccept(unused -> {
                        log.info("[{}] Successfully granted permission on subscription for role {}:{} - "
                                + "namespaceName {}", clientAppId(), roles, subscription, namespaceName);
                    })
                    .exceptionally(ex -> {
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        //The IllegalArgumentException and the IllegalStateException were historically thrown by the
                        // grantPermissionAsync method, so we catch them here to ensure backwards compatibility.
                        if (realCause.getCause() instanceof IllegalArgumentException) {
                            log.warn("[{}] Failed to set permissions for namespace {}: does not exist", clientAppId(),
                                    namespaceName);
                            throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
                        } else if (realCause.getCause() instanceof IllegalStateException) {
                            log.warn("[{}] Failed to set permissions for namespace {}: concurrent modification",
                                    clientAppId(), namespaceName);
                            throw new RestException(Status.CONFLICT, "Concurrent modification");
                        } else {
                            log.error("[{}] Failed to get permissions for namespace {}",
                                    clientAppId(), namespaceName, realCause);
                            throw new RestException(realCause);
                        }
                    });
        } else {
            String msg = "Authorization is not enabled";
            return FutureUtil.failedFuture(new RestException(Status.NOT_IMPLEMENTED, msg));
        }
    }

    protected CompletableFuture<Void> internalRevokePermissionsOnNamespaceAsync(String role) {
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.REVOKE_PERMISSION)
                .thenAccept(__ -> checkNotNull(role, "Role should not be null"))
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.auth_policies.getNamespaceAuthentication().remove(role);
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalRevokePermissionsOnSubscriptionAsync(String subscriptionName,
                                                                                  String role) {
        AuthorizationService authService = pulsar().getBrokerService().getAuthorizationService();
        if (null != authService) {
            return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.REVOKE_PERMISSION)
                    .thenAccept(__ -> {
                        checkNotNull(subscriptionName, "SubscriptionName should not be null");
                        checkNotNull(role, "Role should not be null");
                    })
                    .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                    .thenCompose(__ -> authService.revokeSubscriptionPermissionAsync(namespaceName,
                            subscriptionName, role, null/* additional auth-data json */));
        } else {
            String msg = "Authorization is not enabled";
            return FutureUtil.failedFuture(new RestException(Status.NOT_IMPLEMENTED, msg));
        }
    }

    protected CompletableFuture<Set<String>> internalGetNamespaceReplicationClustersAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.REPLICATION, PolicyOperation.READ)
                .thenAccept(__ -> {
                    if (!namespaceName.isGlobal()) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Cannot get the replication clusters for a non-global namespace");
                    }
                }).thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.replication_clusters);
    }

    @SuppressWarnings("checkstyle:WhitespaceAfter")
    protected CompletableFuture<Void> internalSetNamespaceReplicationClusters(List<String> clusterIds) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.REPLICATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenApply(__ -> {
                    checkNotNull(clusterIds, "ClusterIds should not be null");
                    if (!namespaceName.isGlobal()) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Cannot set replication on a non-global namespace");
                    }
                    Set<String> replicationClusterSet = Sets.newHashSet(clusterIds);
                    if (replicationClusterSet.contains("global")) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Cannot specify global in the list of replication clusters");
                    }
                    return replicationClusterSet;
                }).thenCompose(replicationClusterSet -> clustersAsync()
                        .thenCompose(clusters -> {
                            List<CompletableFuture<Void>> futures =
                                    replicationClusterSet.stream().map(clusterId -> {
                                        if (!clusters.contains(clusterId)) {
                                            throw new RestException(Status.FORBIDDEN,
                                                    "Invalid cluster id: " + clusterId);
                                        }
                                        return validatePeerClusterConflictAsync(clusterId, replicationClusterSet)
                                                .thenCompose(__ ->
                                                        validateClusterForTenantAsync(
                                                                namespaceName.getTenant(), clusterId));
                                    }).collect(Collectors.toList());
                            return FutureUtil.waitForAll(futures).thenApply(__ -> replicationClusterSet);
                        }))
                .thenCompose(replicationClusterSet -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.replication_clusters = replicationClusterSet;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetNamespaceMessageTTLAsync(Integer messageTTL) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.TTL, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> {
                    if (messageTTL != null && messageTTL < 0) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Invalid value for message TTL, message TTL must >= 0");
                    }
                }).thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.message_ttl_in_seconds = messageTTL;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetSubscriptionExpirationTimeAsync(Integer expirationTime) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SUBSCRIPTION_EXPIRATION_TIME,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> {
                    if (expirationTime != null && expirationTime < 0) {
                        throw new RestException(Status.PRECONDITION_FAILED,
                                "Invalid value for subscription expiration time");
                    }
                }).thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.subscription_expiration_time_minutes = expirationTime;
                    return policies;
                }));
    }

    protected CompletableFuture<AutoTopicCreationOverride> internalGetAutoTopicCreationAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.AUTO_TOPIC_CREATION,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.autoTopicCreationOverride);
    }

    protected CompletableFuture<Void> internalSetAutoTopicCreationAsync(
            AutoTopicCreationOverride autoTopicCreationOverride) {
        return validateNamespacePolicyOperationAsync(namespaceName,
                PolicyName.AUTO_TOPIC_CREATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenAccept(__ -> {
                    int maxPartitions = pulsar().getConfig().getMaxNumPartitionsPerPartitionedTopic();
                    if (autoTopicCreationOverride != null) {
                        ValidateResult validateResult =
                                AutoTopicCreationOverrideImpl.validateOverride(autoTopicCreationOverride);
                        if (!validateResult.isSuccess()) {
                            throw new RestException(Status.PRECONDITION_FAILED,
                                    "Invalid configuration for autoTopicCreationOverride. the detail is "
                                            + validateResult.getErrorInfo());
                        }
                        if (Objects.equals(autoTopicCreationOverride.getTopicType(),
                                                                  TopicType.PARTITIONED.toString())){
                            if (maxPartitions > 0
                                    && autoTopicCreationOverride.getDefaultNumPartitions() > maxPartitions) {
                                throw new RestException(Status.NOT_ACCEPTABLE,
                                        "Number of partitions should be less than or equal to " + maxPartitions);
                            }

                        }
                    }
                })
                .thenCompose(__ -> namespaceResources().setPoliciesAsync(namespaceName, policies -> {
                    policies.autoTopicCreationOverride = autoTopicCreationOverride;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetAutoSubscriptionCreationAsync(AutoSubscriptionCreationOverride
                                                               autoSubscriptionCreationOverride) {
        // Force to read the data s.t. the watch to the cache content is setup.
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.AUTO_SUBSCRIPTION_CREATION,
                PolicyOperation.WRITE)
                .thenCompose(__ ->  validatePoliciesReadOnlyAccessAsync())
                        .thenCompose(unused -> namespaceResources().setPoliciesAsync(namespaceName, policies -> {
                            policies.autoSubscriptionCreationOverride = autoSubscriptionCreationOverride;
                            return policies;
                        }))
                .thenAccept(r -> {
                    if (autoSubscriptionCreationOverride != null) {
                        String autoOverride = autoSubscriptionCreationOverride.isAllowAutoSubscriptionCreation()
                                ? "enabled" : "disabled";
                        log.info("[{}] Successfully {} autoSubscriptionCreation on namespace {}", clientAppId(),
                                autoOverride, namespaceName);
                    } else {
                        log.info("[{}] Successfully remove autoSubscriptionCreation on namespace {}",
                                clientAppId(), namespaceName);
                    }
                });
    }

    protected CompletableFuture<AutoSubscriptionCreationOverride> internalGetAutoSubscriptionCreationAsync() {

        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.AUTO_SUBSCRIPTION_CREATION,
                PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.autoSubscriptionCreationOverride);
    }

    protected CompletableFuture<Void> internalModifyDeduplicationAsync(Boolean enableDeduplication) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DEDUPLICATION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.deduplicationEnabled = enableDeduplication;
                    return policies;
                }));
    }

    @SuppressWarnings("deprecation")
    protected void internalUnloadNamespace(AsyncResponse asyncResponse) {
        validateSuperUserAccess();
        log.info("[{}] Unloading namespace {}", clientAppId(), namespaceName);

        if (namespaceName.isGlobal()) {
            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
            validateGlobalNamespaceOwnership(namespaceName);
        } else {
            validateClusterOwnership(namespaceName.getCluster());
            validateClusterForTenant(namespaceName.getTenant(), namespaceName.getCluster());
        }

        Policies policies = getNamespacePolicies(namespaceName);

        final List<CompletableFuture<Void>> futures = Lists.newArrayList();
        List<String> boundaries = policies.bundles.getBoundaries();
        for (int i = 0; i < boundaries.size() - 1; i++) {
            String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
            try {
                futures.add(pulsar().getAdminClient().namespaces().unloadNamespaceBundleAsync(namespaceName.toString(),
                        bundle));
            } catch (PulsarServerException e) {
                log.error("[{}] Failed to unload namespace {}", clientAppId(), namespaceName, e);
                asyncResponse.resume(new RestException(e));
                return;
            }
        }

        FutureUtil.waitForAll(futures).handle((result, exception) -> {
            if (exception != null) {
                log.error("[{}] Failed to unload namespace {}", clientAppId(), namespaceName, exception);
                if (exception.getCause() instanceof PulsarAdminException) {
                    asyncResponse.resume(new RestException((PulsarAdminException) exception.getCause()));
                    return null;
                } else {
                    asyncResponse.resume(new RestException(exception.getCause()));
                    return null;
                }
            }
            log.info("[{}] Successfully unloaded all the bundles in namespace {}", clientAppId(), namespaceName);
            asyncResponse.resume(Response.noContent().build());
            return null;
        });
    }

    protected CompletableFuture<Void> internalUnloadNamespaceAsync() {
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> {
                    log.info("[{}] Unloading namespace {}", clientAppId(), namespaceName);
                    if (namespaceName.isGlobal()) {
                        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                        return validateGlobalNamespaceOwnershipAsync(namespaceName);
                    } else {
                        return validateClusterOwnershipAsync(namespaceName.getCluster())
                                .thenCompose(ignore -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                        namespaceName.getCluster()));
                    }
                })
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies -> {
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();
                    List<String> boundaries = policies.bundles.getBoundaries();
                    for (int i = 0; i < boundaries.size() - 1; i++) {
                        String bundle = String.format("%s_%s", boundaries.get(i), boundaries.get(i + 1));
                        try {
                            futures.add(pulsar().getAdminClient().namespaces().unloadNamespaceBundleAsync(
                                    namespaceName.toString(), bundle));
                        } catch (PulsarServerException e) {
                            log.error("[{}] Failed to unload namespace {}", clientAppId(), namespaceName, e);
                            throw new RestException(e);
                        }
                    }
                    return FutureUtil.waitForAll(futures);
                });
    }


    protected CompletableFuture<Void> internalSetBookieAffinityGroupAsync(BookieAffinityGroupData bookieAffinityGroup) {
        return validateSuperUserAccessAsync().thenAccept(__ -> log.info("[{}] Setting bookie-affinity-group {} for"
                        + "namespace {}", clientAppId(), bookieAffinityGroup,
                this.namespaceName)).thenCompose(__ -> {
            if (namespaceName.isGlobal()) {
                // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                return validateGlobalNamespaceOwnershipAsync(namespaceName);
            } else {
                return validateClusterOwnershipAsync(namespaceName.getCluster())
                        .thenCompose(ignore -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                namespaceName.getCluster()));

            }

        }).thenCompose(__ -> getLocalPolicies().setLocalPoliciesWithCreateAsync(namespaceName, oldPolicies -> {
            LocalPolicies localPolicies = oldPolicies.map(
                            policies -> new LocalPolicies(policies.bundles,
                                    bookieAffinityGroup,
                                    policies.namespaceAntiAffinityGroup))
                    .orElseGet(() -> new LocalPolicies(defaultBundle(),
                            bookieAffinityGroup,
                            null));
            log.info("[{}] Successfully updated local-policies configuration: namespace={}, map={}", clientAppId(),
                    namespaceName, localPolicies);
            return localPolicies;
        }));
    }

    protected CompletableFuture<BookieAffinityGroupData> internalGetBookieAffinityGroupAsync() {
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> {
                    if (namespaceName.isGlobal()) {
                        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                        return validateGlobalNamespaceOwnershipAsync(namespaceName);
                    } else {
                        return validateClusterOwnershipAsync(namespaceName.getCluster())
                                .thenCompose(ignore -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                        namespaceName.getCluster()));

                    }

                }).thenCompose(__ -> getLocalPolicies().getLocalPoliciesAsync(namespaceName)
                        .thenCompose(optPolicies -> {
                            if(optPolicies.isEmpty()) {
                                throw new RestException(Status.NOT_FOUND, "Namespace local-policies does not "
                                        + "exist");
                            }
                            return CompletableFuture.completedFuture(optPolicies.get().bookieAffinityGroup);
                        }));
    }

    public CompletableFuture<Void> internalUnloadNamespaceBundleAsync(String bundleRange, boolean authoritative) {
        return validateSuperUserAccessAsync()
                .thenAccept(__ -> {
                    checkNotNull(bundleRange, "BundleRange should not be null");
                    log.info("[{}] Unloading namespace bundle {}/{}", clientAppId(), namespaceName, bundleRange);
                })
                .thenApply(__ ->
                    pulsar().getNamespaceService().getNamespaceBundleFactory()
                                    .getBundle(namespaceName.toString(), bundleRange)
                )
                .thenCompose(bundle ->
                    pulsar().getNamespaceService().isNamespaceBundleOwned(bundle)
                          .exceptionally(ex -> {
                            if (log.isDebugEnabled()) {
                                log.debug("Failed to validate cluster ownership for {}-{}, {}",
                                        namespaceName.toString(), bundleRange, ex.getMessage(), ex);
                            }
                            return false;
                          })
                )
                .thenCompose(isOwnedByLocalCluster -> {
                    if (!isOwnedByLocalCluster) {
                        if (namespaceName.isGlobal()) {
                            // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                            return validateGlobalNamespaceOwnershipAsync(namespaceName);
                        } else {
                            return validateClusterOwnershipAsync(namespaceName.getCluster())
                                    .thenCompose(__ -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                            namespaceName.getCluster()));
                        }
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                })
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies ->
                     isBundleOwnedByAnyBroker(namespaceName, policies.bundles, bundleRange)
                        .thenCompose(flag -> {
                            if (!flag) {
                                log.info("[{}] Namespace bundle is not owned by any broker {}/{}", clientAppId(),
                                        namespaceName, bundleRange);
                                return CompletableFuture.completedFuture(null);
                            }
                            return validateNamespaceBundleOwnershipAsync(namespaceName, policies.bundles, bundleRange,
                                    authoritative, true)
                                    .thenCompose(nsBundle ->
                                            pulsar().getNamespaceService().unloadNamespaceBundle(nsBundle));
                        }));
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalSplitNamespaceBundleAsync(String bundleName,
                                                                        boolean authoritative, boolean unload,
                                                                        String splitAlgorithmName,
                                                                        List<Long> splitBoundaries) {
        return validateSuperUserAccessAsync()
                .thenAccept(__ -> {
                    checkNotNull(bundleName, "BundleRange should not be null");
                    log.info("[{}] Split namespace bundle {}/{}", clientAppId(), namespaceName, bundleName);
                    List<String> supportedNamespaceBundleSplitAlgorithms =
                            pulsar().getConfig().getSupportedNamespaceBundleSplitAlgorithms();
                    if (StringUtils.isNotBlank(splitAlgorithmName)) {
                        if (!supportedNamespaceBundleSplitAlgorithms.contains(splitAlgorithmName)) {
                            throw new RestException(Status.PRECONDITION_FAILED,
                                    "Unsupported namespace bundle split algorithm, supported algorithms are "
                                            + supportedNamespaceBundleSplitAlgorithms);
                        }
                        if (splitAlgorithmName
                                .equalsIgnoreCase(NamespaceBundleSplitAlgorithm.SPECIFIED_POSITIONS_DIVIDE)
                                && (splitBoundaries == null || splitBoundaries.size() == 0)) {
                            throw new RestException(Status.PRECONDITION_FAILED,
                                    "With specified_positions_divide split algorithm, splitBoundaries must not be "
                                            + "empty");
                        }
                    }
                })
                .thenCompose(__ -> {
                    if (namespaceName.isGlobal()) {
                        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                        return validateGlobalNamespaceOwnershipAsync(namespaceName);
                    } else {
                        return validateClusterOwnershipAsync(namespaceName.getCluster())
                                .thenCompose(ignore -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                        namespaceName.getCluster()));
                    }
                })
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies->{
                    String bundleRange = getBundleRange(bundleName);
                    return validateNamespaceBundleOwnershipAsync(namespaceName, policies.bundles, bundleRange,
                            authoritative, false)
                            .thenCompose(nsBundle -> pulsar().getNamespaceService().splitAndOwnBundle(nsBundle, unload,
                                    getNamespaceBundleSplitAlgorithmByName(splitAlgorithmName), splitBoundaries));

                });
    }

    protected CompletableFuture<TopicHashPositions> internalGetTopicHashPositionsAsync(String bundleRange,
                                                                                       List<String> topics) {
        if (log.isDebugEnabled()) {
            log.debug("[{}] Getting hash position for topic list {}, bundle {}", clientAppId(), topics, bundleRange);
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.PERSISTENCE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenCompose(policies -> {
                    return validateNamespaceBundleOwnershipAsync(namespaceName, policies.bundles, bundleRange,
                            false, true)
                            .thenCompose(nsBundle ->
                                    pulsar().getNamespaceService().getOwnedTopicListForNamespaceBundle(nsBundle))
                            .thenApply(allTopicsInThisBundle -> {
                                Map<String, Long> topicHashPositions = new HashMap<>();
                                if (topics == null || topics.size() == 0) {
                                    allTopicsInThisBundle.forEach(t -> {
                                        topicHashPositions.put(t,
                                                pulsar().getNamespaceService().getNamespaceBundleFactory()
                                                        .getLongHashCode(t));
                                    });
                                } else {
                                    for (String topic : topics.stream().map(Codec::decode).toList()) {
                                        TopicName topicName = TopicName.get(topic);
                                        // partitioned topic
                                        if (topicName.getPartitionIndex() == -1) {
                                            allTopicsInThisBundle.stream()
                                                    .filter(t -> TopicName.get(t).getPartitionedTopicName()
                                                            .equals(TopicName.get(topic).getPartitionedTopicName()))
                                                    .forEach(partition -> {
                                                        topicHashPositions.put(partition,
                                                                pulsar().getNamespaceService()
                                                                        .getNamespaceBundleFactory()
                                                                        .getLongHashCode(partition));
                                                    });
                                        } else { // topic partition
                                            if (allTopicsInThisBundle.contains(topicName.toString())) {
                                                topicHashPositions.put(topic,
                                                        pulsar().getNamespaceService().getNamespaceBundleFactory()
                                                                .getLongHashCode(topic));
                                            }
                                        }
                                    }
                                }
                                return new TopicHashPositions(namespaceName.toString(), bundleRange,
                                        topicHashPositions);
                            });
                });
    }

    private String getBundleRange(String bundleName) {
        if (BundleType.LARGEST.toString().equals(bundleName)) {
            return findLargestBundleWithTopics(namespaceName).getBundleRange();
        } else if (BundleType.HOT.toString().equals(bundleName)) {
            return findHotBundle(namespaceName).getBundleRange();
        } else {
            return bundleName;
        }
    }

    private NamespaceBundle findLargestBundleWithTopics(NamespaceName namespaceName) {
        return pulsar().getNamespaceService().getNamespaceBundleFactory().getBundleWithHighestTopics(namespaceName);
    }

    private NamespaceBundle findHotBundle(NamespaceName namespaceName) {
        return pulsar().getNamespaceService().getNamespaceBundleFactory().getBundleWithHighestThroughput(namespaceName);
    }

    private NamespaceBundleSplitAlgorithm getNamespaceBundleSplitAlgorithmByName(String algorithmName) {
        NamespaceBundleSplitAlgorithm algorithm = NamespaceBundleSplitAlgorithm.of(algorithmName);
        if (algorithm == null) {
            algorithm = NamespaceBundleSplitAlgorithm.of(
                    pulsar().getConfig().getDefaultNamespaceBundleSplitAlgorithm());
        }
        if (algorithm == null) {
            algorithm = NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO;
        }
        return algorithm;
    }

    protected void internalSetPublishRate(PublishRate maxPublishMessageRate) {
        validateSuperUserAccess();
        log.info("[{}] Set namespace publish-rate {}/{}", clientAppId(), namespaceName, maxPublishMessageRate);
        updatePolicies(namespaceName, policies -> {
            policies.publishMaxMessageRate.put(pulsar().getConfiguration().getClusterName(), maxPublishMessageRate);
            return policies;
        });
        log.info("[{}] Successfully updated the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                namespaceName);
    }

    protected CompletableFuture<Void> internalSetPublishRateAsync(PublishRate maxPublishMessageRate) {
        log.info("[{}] Set namespace publish-rate {}/{}", clientAppId(), namespaceName, maxPublishMessageRate);
        return validateSuperUserAccessAsync().thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.publishMaxMessageRate.put(pulsar().getConfiguration().getClusterName(), maxPublishMessageRate);
            log.info("[{}] Successfully updated the publish_max_message_rate for cluster on namespace {}",
                    clientAppId(), namespaceName);
            return policies;
        }));
    }

    protected void internalRemovePublishRate() {
        validateSuperUserAccess();
        log.info("[{}] Remove namespace publish-rate {}/{}", clientAppId(), namespaceName, topicName);
        try {
            updatePolicies(namespaceName, policies -> {
                if (policies.publishMaxMessageRate != null) {
                    policies.publishMaxMessageRate.remove(pulsar().getConfiguration().getClusterName());
                }
                return policies;
            });
            log.info("[{}] Successfully remove the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                    namespaceName);
        } catch (Exception e) {
            log.error("[{}] Failed to remove the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> internalRemovePublishRateAsync() {
        log.info("[{}] Remove namespace publish-rate {}/{}", clientAppId(), namespaceName, topicName);
        return validateSuperUserAccessAsync().thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            if (policies.publishMaxMessageRate != null) {
                policies.publishMaxMessageRate.remove(pulsar().getConfiguration().getClusterName());
            }
            log.info("[{}] Successfully remove the publish_max_message_rate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }

    protected CompletableFuture<PublishRate> internalGetPublishRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies ->
                        policies.publishMaxMessageRate.get(pulsar().getConfiguration().getClusterName()));
    }

    @SuppressWarnings("deprecation")
    protected void internalSetTopicDispatchRate(DispatchRateImpl dispatchRate) {
        validateSuperUserAccess();
        log.info("[{}] Set namespace dispatch-rate {}/{}", clientAppId(), namespaceName, dispatchRate);
        try {
            updatePolicies(namespaceName, policies -> {
                policies.topicDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
                policies.clusterDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
                return policies;
            });
            log.info("[{}] Successfully updated the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
        } catch (Exception e) {
            log.error("[{}] Failed to update the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalSetTopicDispatchRateAsync(DispatchRateImpl dispatchRate) {
        log.info("[{}] Set namespace dispatch-rate {}/{}", clientAppId(), namespaceName, dispatchRate);
        return validateSuperUserAccessAsync().thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.topicDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
            policies.clusterDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
            log.info("[{}] Successfully updated the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }

    protected void internalDeleteTopicDispatchRate() {
        validateSuperUserAccess();
        try {
            updatePolicies(namespaceName, policies -> {
                policies.topicDispatchRate.remove(pulsar().getConfiguration().getClusterName());
                policies.clusterDispatchRate.remove(pulsar().getConfiguration().getClusterName());
                return policies;
            });
            log.info("[{}] Successfully delete the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
        } catch (Exception e) {
            log.error("[{}] Failed to delete the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName, e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> internalDeleteTopicDispatchRateAsync() {
        return validateSuperUserAccessAsync().thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.topicDispatchRate.remove(pulsar().getConfiguration().getClusterName());
            policies.clusterDispatchRate.remove(pulsar().getConfiguration().getClusterName());
            log.info("[{}] Successfully delete the dispatchRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<DispatchRate> internalGetTopicDispatchRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.topicDispatchRate.get(pulsar().getConfiguration().getClusterName()));
    }

    protected CompletableFuture<Void> internalSetSubscriptionDispatchRateAsync(DispatchRateImpl dispatchRate) {
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.subscriptionDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
                    log.info("[{}] Successfully updated the subscriptionDispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName);
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalDeleteSubscriptionDispatchRateAsync() {
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.subscriptionDispatchRate.remove(pulsar().getConfiguration().getClusterName());
                    log.info("[{}] Successfully delete the subscriptionDispatchRate for cluster on namespace {}",
                            clientAppId(), namespaceName);
                    return policies;
                }));
    }

    protected CompletableFuture<DispatchRate> internalGetSubscriptionDispatchRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies ->
                        policies.subscriptionDispatchRate.get(pulsar().getConfiguration().getClusterName()));
    }

    protected CompletableFuture<Void> internalSetSubscribeRateAsync(SubscribeRate subscribeRate) {
        log.info("[{}] Set namespace subscribe-rate {}/{}", clientAppId(), namespaceName, subscribeRate);
        return validateSuperUserAccessAsync().thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.clusterSubscribeRate.put(pulsar().getConfiguration().getClusterName(), subscribeRate);
            log.info("[{}] Successfully updated the subscribeRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }

    protected CompletableFuture<Void> internalDeleteSubscribeRateAsync() {
        return validateSuperUserAccessAsync().thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
            policies.clusterSubscribeRate.remove(pulsar().getConfiguration().getClusterName());
            log.info("[{}] Successfully delete the subscribeRate for cluster on namespace {}", clientAppId(),
                    namespaceName);
            return policies;
        }));
    }


    protected CompletableFuture<SubscribeRate> internalGetSubscribeRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RATE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.clusterSubscribeRate.get(pulsar().getConfiguration().getClusterName()));
    }

    protected CompletableFuture<Void> internalRemoveReplicatorDispatchRateAsync() {
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.replicatorDispatchRate.remove(pulsar().getConfiguration().getClusterName());
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetReplicatorDispatchRateAsync(DispatchRateImpl dispatchRate) {
        return validateSuperUserAccessAsync()
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    log.info("[{}] Set namespace replicator dispatch-rate {}/{}", clientAppId(), namespaceName, dispatchRate);
                    policies.replicatorDispatchRate.put(pulsar().getConfiguration().getClusterName(), dispatchRate);
                    return policies;
                }));
    }

    protected CompletableFuture<DispatchRate> internalGetReplicatorDispatchRateAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.REPLICATION_RATE, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(
                        policies -> policies.replicatorDispatchRate.get(pulsar().getConfiguration().getClusterName()));
    }

    protected CompletableFuture<Void> internalSetBacklogQuota(BacklogQuotaType backlogQuotaType,
                                                              BacklogQuota backlogQuota) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.BACKLOG, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    final BacklogQuotaType quotaType = backlogQuotaType != null ? backlogQuotaType
                            : BacklogQuotaType.destination_storage;
                    return namespaceResources().getPoliciesAsync(namespaceName)
                            .thenCompose(optPolicies -> {
                                if (optPolicies.isEmpty()) {
                                    throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
                                }
                                Policies policies = optPolicies.get();
                                RetentionPolicies r = policies.retention_policies;
                                if (r != null) {
                                    Policies p = new Policies();
                                    p.backlog_quota_map.put(quotaType, backlogQuota);
                                    if (!checkQuotas(p, r)) {
                                        log.warn(
                                                "[{}] Failed to update backlog configuration"
                                                        + " for namespace {}: conflicts with retention quota",
                                                clientAppId(), namespaceName);
                                        throw new RestException(Status.PRECONDITION_FAILED,
                                                "Backlog Quota exceeds configured retention quota for namespace."
                                                        + " Please increase retention quota and retry");
                                    }
                                }
                                policies.backlog_quota_map.put(quotaType, backlogQuota);
                                return namespaceResources().setPoliciesAsync(namespaceName, p -> policies);
                            });

                });
    }

    protected CompletableFuture<Void> internalRemoveBacklogQuotaAsync(BacklogQuotaType backlogQuotaType) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.BACKLOG, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    final BacklogQuotaType quotaType = backlogQuotaType != null ? backlogQuotaType
                            : BacklogQuotaType.destination_storage;
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.backlog_quota_map.remove(quotaType);
                        return policies;
                    });
                });
    }

    protected CompletableFuture<Void> internalSetRetentionAsync(RetentionPolicies retention) {
        validateRetentionPolicies(retention);
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RETENTION, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> namespaceResources().getPoliciesAsync(namespaceName)
                        .thenCompose(optPolicies -> {
                            if (optPolicies.isEmpty()) {
                                throw new RestException(Status.NOT_FOUND, "Namespace does not exist");
                            }
                            Policies policies = optPolicies.get();
                            RetentionPolicies r = policies.retention_policies;
                            if (r != null) {
                                if (!checkQuotas(policies, r)) {
                                    log.warn("[{}] Failed to update retention configuration"
                                                    + " for namespace {}: conflicts with backlog quota",
                                            clientAppId(), namespaceName);
                                    throw new RestException(Status.PRECONDITION_FAILED,
                                            "Retention Quota must exceed configured backlog quota for namespace.");
                                }
                            }
                            return namespaceResources().setPoliciesAsync(namespaceName, p -> policies);
                        }));
    }

    protected CompletableFuture<Void> internalSetPersistence(PersistencePolicies persistence) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.PERSISTENCE, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.persistence = persistence;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalClearNamespaceBacklogAsync() {
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.CLEAR_BACKLOG)
                .thenCompose(__ -> {
                    NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                            .getBundles(namespaceName);
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();
                    for (NamespaceBundle bundle : bundles.getBundles()) {
                        // check if the bundle is owned by any broker, if not then we do not need to delete the bundle
                        futures.add(pulsar().getNamespaceService()
                                .getOwnerAsync(bundle).thenCompose(ownership -> {
                                    if (ownership.isPresent()) {
                                        try {
                                            return pulsar().getAdminClient().namespaces()
                                                    .clearNamespaceBundleBacklogAsync(namespaceName.toString(),
                                                            bundle.getBundleRange());
                                        } catch (PulsarServerException e) {
                                            throw new RestException(e);
                                        }
                                    } else {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                }));
                    }
                    return FutureUtil.waitForAll(futures).exceptionally(exception -> {
                        Throwable realCause = FutureUtil.unwrapCompletionException(exception);
                        log.error("[{}] Failed to clear backlog on the bundles for namespace {}: {}", clientAppId(),
                                namespaceName, exception.getCause().getMessage());
                        if (realCause instanceof PulsarAdminException) {
                            throw new RestException((PulsarAdminException) exception.getCause());
                        } else {
                            throw new RestException(exception.getCause());
                        }
                    });

                });
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalClearNamespaceBundleBacklogAsync(String bundleRange,
                                                                               String subscriptionName,
                                                                               boolean authoritative) {
        checkNotNull(bundleRange, "BundleRange should not be null");
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.CLEAR_BACKLOG)
                .thenCompose(__ -> {
                    if (namespaceName.isGlobal()) {
                        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                        return validateGlobalNamespaceOwnershipAsync(namespaceName);
                    } else {
                        return validateClusterOwnershipAsync(namespaceName.getCluster())
                                .thenCompose(ignore -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                        namespaceName.getCluster()));
                    }
                }).thenCompose(__ -> getNamespacePoliciesAsync(namespaceName).thenCompose(policies ->
                        validateNamespaceBundleOwnershipAsync(namespaceName,
                                policies.bundles, bundleRange, authoritative, true)))
                .thenCompose(__ -> clearBacklog(namespaceName, bundleRange, subscriptionName));
    }

    protected CompletableFuture<Void> internalClearNamespaceBacklogForSubscriptionAsync(String subscription) {
        checkNotNull(subscription, "Subscription should not be null");
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.CLEAR_BACKLOG)
                .thenCompose(__ -> {
                    NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                            .getBundles(namespaceName);
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                    for (NamespaceBundle nsBundle : bundles.getBundles()) {
                        // check if the bundle is owned by any broker, if not then we do not need to delete the bundle
                        futures.add(pulsar().getNamespaceService()
                                .getOwnerAsync(nsBundle).thenCompose(ownership -> {
                                    if (ownership.isPresent()) {
                                        try {
                                            return pulsar().getAdminClient().namespaces()
                                                    .clearNamespaceBundleBacklogForSubscriptionAsync(
                                                            namespaceName.toString(), nsBundle.getBundleRange(), subscription);
                                        } catch (PulsarServerException e) {
                                            throw new RestException(e);
                                        }
                                    } else {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                }));
                    }
                    return FutureUtil.waitForAll(futures).exceptionally(exception -> {
                        Throwable realCause = FutureUtil.unwrapCompletionException(exception);
                        if (realCause instanceof PulsarAdminException) {
                            throw new RestException((PulsarAdminException) exception.getCause());
                        } else {
                            throw new RestException(exception.getCause());
                        }
                    });

                });
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalClearNamespaceBundleBacklogForSubscriptionAsync(String subscription,
                                                                                              String bundleRange,
                                                                                              boolean authoritative) {
        checkNotNull(subscription, "Subscription should not be null");
        checkNotNull(bundleRange, "BundleRange should not be null");
        return internalClearNamespaceBundleBacklogAsync(bundleRange, subscription, authoritative);
    }

    protected CompletableFuture<Void> internalUnsubscribeNamespaceAsync(String subscription) {
        checkNotNull(subscription, "Subscription should not be null");
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.CLEAR_BACKLOG)
                .thenCompose(__ -> {
                    NamespaceBundles bundles = pulsar().getNamespaceService().getNamespaceBundleFactory()
                            .getBundles(namespaceName);
                    final List<CompletableFuture<Void>> futures = Lists.newArrayList();

                    for (NamespaceBundle nsBundle : bundles.getBundles()) {
                        // check if the bundle is owned by any broker, if not then we do not need to delete the bundle
                        futures.add(pulsar().getNamespaceService()
                                .getOwnerAsync(nsBundle).thenCompose(ownership -> {
                                    if (ownership.isPresent()) {
                                        try {
                                            return pulsar().getAdminClient().namespaces()
                                                    .unsubscribeNamespaceBundleAsync(namespaceName.toString(),
                                                            nsBundle.getBundleRange(), subscription);
                                        } catch (PulsarServerException e) {
                                            throw new RestException(e);
                                        }
                                    } else {
                                        return CompletableFuture.completedFuture(null);
                                    }
                                }));
                    }
                    return FutureUtil.waitForAll(futures).handle((result, exception) -> {
                        if (exception != null) {
                            Throwable realCause = FutureUtil.unwrapCompletionException(exception);
                            log.warn("[{}] Failed to unsubscribeAsync {} on the bundles for namespace {}: {}", clientAppId(),
                                    subscription, namespaceName, exception.getCause().getMessage());
                            if (realCause instanceof PulsarAdminException) {
                                throw new RestException((PulsarAdminException) exception.getCause());
                            } else {
                                throw new RestException(exception.getCause());
                            }
                        }
                        log.info("[{}] Successfully unsubscribed {} on all the bundles for namespace {}", clientAppId(),
                                subscription, namespaceName);
                        return null;
                    });
                });
    }

    @SuppressWarnings("deprecation")
    protected CompletableFuture<Void> internalUnsubscribeNamespaceBundleAsync(String subscription, String bundleRange,
                                                                              boolean authoritative) {
        checkNotNull(subscription, "Subscription should not be null");
        checkNotNull(bundleRange, "BundleRange should not be null");
        return validateNamespaceOperationAsync(namespaceName, NamespaceOperation.UNSUBSCRIBE)
                .thenCompose(__ -> {
                    if (namespaceName.isGlobal()) {
                        // check cluster ownership for a given global namespace: redirect if peer-cluster owns it
                        return validateGlobalNamespaceOwnershipAsync(namespaceName);
                    } else {
                        return validateClusterOwnershipAsync(namespaceName.getCluster())
                                .thenCompose(ignore -> validateClusterForTenantAsync(namespaceName.getTenant(),
                                        namespaceName.getCluster()));
                    }
                }).thenCompose(__ -> getNamespacePoliciesAsync(namespaceName).thenCompose(policies ->
                        validateNamespaceBundleOwnershipAsync(namespaceName,
                                policies.bundles, bundleRange, authoritative, true)))
                .thenCompose(__ -> unsubscribeAsync(namespaceName, bundleRange, subscription)
                        .exceptionally(exception -> {
                            Throwable realCause = FutureUtil.unwrapCompletionException(exception);
                            log.error("[{}] Failed to unsubscribeAsync {} for namespace {}/{}",
                                    clientAppId(), subscription, namespaceName, bundleRange, exception);
                            if (realCause instanceof BrokerServiceException.SubscriptionBusyException) {
                                throw new RestException(Status.PRECONDITION_FAILED,
                                        "Subscription has active connected consumers");
                            }
                            throw new RestException(exception.getCause());
                }));
    }

    protected CompletableFuture<Void> internalSetSubscriptionAuthModeAsync(SubscriptionAuthMode subscriptionAuthMode) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SUBSCRIPTION_AUTH_MODE,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    final SubscriptionAuthMode authMode = subscriptionAuthMode != null ? subscriptionAuthMode
                            : SubscriptionAuthMode.None;
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.subscription_auth_mode = authMode;
                        return policies;
                    });
                });
    }

    protected CompletableFuture<Void> internalModifyEncryptionRequiredAsync(boolean encryptionRequired) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ENCRYPTION,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.encryption_required = encryptionRequired;
                        return policies;
                    });
                });
    }

    protected Boolean internalGetEncryptionRequired() {
        validateNamespacePolicyOperation(namespaceName, PolicyName.ENCRYPTION, PolicyOperation.READ);
        Policies policies = getNamespacePolicies(namespaceName);
        return policies.encryption_required;
    }

    protected CompletableFuture<Void> internalSetInactiveTopic(InactiveTopicPolicies inactiveTopicPolicies) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.INACTIVE_TOPIC,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.inactive_topic_policies = inactiveTopicPolicies;
                        return policies;
                    });
                });
    }

    protected void internalSetPolicies(String fieldName, Object value) {
        try {
            Policies policies = namespaceResources().getPolicies(namespaceName)
                    .orElseThrow(() -> new RestException(Status.NOT_FOUND,
                    "Namespace policies does not exist"));
            Field field = Policies.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(policies, value);
            namespaceResources().setPolicies(namespaceName, p -> policies);
            log.info("[{}] Successfully updated {} configuration: namespace={}, value={}", clientAppId(), fieldName,
                    namespaceName, jsonMapper().writeValueAsString(value));

        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update {} configuration for namespace {}", clientAppId(), fieldName
                    , namespaceName, e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> internalSetDelayedDeliveryAsync(DelayedDeliveryPolicies delayedDeliveryPolicies) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DELAYED_DELIVERY,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.delayed_delivery_policies = delayedDeliveryPolicies;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetNamespaceAntiAffinityGroupAsync(String antiAffinityGroup) {
        checkNotNull(antiAffinityGroup, "AntiAffinityGroup should not be null");
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ANTI_AFFINITY,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> getLocalPolicies().setLocalPoliciesWithCreateAsync(namespaceName,
                        (lp)-> lp.map(policies -> new LocalPolicies(policies.bundles,
                                policies.bookieAffinityGroup, antiAffinityGroup))
                                .orElseGet(() -> new LocalPolicies(defaultBundle(), null, antiAffinityGroup))));
    }

    protected CompletableFuture<String> internalGetNamespaceAntiAffinityGroupAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ANTI_AFFINITY,
                PolicyOperation.READ)
                .thenCompose(__ -> getLocalPolicies().getLocalPoliciesAsync(namespaceName)
                        .thenCompose(optPolicies -> {
                            if(optPolicies.isEmpty()) {
                                throw new RestException(Status.NOT_FOUND, "Namespace local-policies does not "
                                        + "exist");
                            }
                            return CompletableFuture.completedFuture(optPolicies.get().namespaceAntiAffinityGroup);
                        }));
    }

    protected CompletableFuture<List<String>> internalGetAntiAffinityNamespacesAsync(String cluster,
                                                                                     String antiAffinityGroup,
                                                                                     String tenant) {
        checkNotNull(cluster, "Cluster should not be null");
        checkNotNull(antiAffinityGroup, "AntiAffinityGroup should not be null");
        checkNotNull(tenant, "Tenant should not be null");
        if (isBlank(antiAffinityGroup)) {
            throw new RestException(Status.PRECONDITION_FAILED, "anti-affinity group can't be empty.");
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ANTI_AFFINITY, PolicyOperation.READ)
                .thenCompose(__ -> clusterResources().getClusterAsync(cluster))
                .thenCompose(optClusterData -> {
                    log.info("[{}]-{} Finding namespaces for {} in {}",
                            clientAppId(), tenant, antiAffinityGroup, cluster);
                    if (optClusterData.isEmpty()) {
                        throw new RestException(Status.PRECONDITION_FAILED, "Cluster " + cluster + " does not exist.");
                    }
                    return tenantResources().getListOfNamespacesAsync(tenant);
                })
                .thenCompose(namespaces -> {
                    List<CompletableFuture<Void>> futures = new ArrayList<>();
                    List<String> ans = new ArrayList<>();
                    for (String ns: namespaces) {
                        futures.add(getLocalPolicies().getLocalPoliciesAsync(NamespaceName.get(ns))
                                .thenAccept(optPolicies -> {
                                    String storedAntiAffinityGroup = optPolicies.orElse(new LocalPolicies())
                                            .namespaceAntiAffinityGroup;
                                    if (antiAffinityGroup.equalsIgnoreCase(storedAntiAffinityGroup)) {
                                        ans.add(ns);
                                    }
                                }));
                    }
                    return FutureUtil.waitForAll(futures)
                            .thenCompose(__ -> CompletableFuture.completedFuture(ans));
                });
    }

    private boolean checkQuotas(Policies policies, RetentionPolicies retention) {
        Map<BacklogQuota.BacklogQuotaType, BacklogQuota> backlogQuotaMap = policies.backlog_quota_map;
        if (backlogQuotaMap.isEmpty()) {
            return true;
        }
        BacklogQuota quota = backlogQuotaMap.get(BacklogQuotaType.destination_storage);
        return checkBacklogQuota(quota, retention);
    }

    private CompletableFuture<Void> clearBacklog(NamespaceName nsName, String bundleRange, String subscription) {
        List<Topic> topicList = pulsar().getBrokerService().getAllTopicsFromNamespaceBundle(nsName.toString(),
                nsName.toString() + "/" + bundleRange);

        List<CompletableFuture<Void>> futures = Lists.newArrayList();
        if (subscription != null) {
            if (subscription.startsWith(pulsar().getConfiguration().getReplicatorPrefix())) {
                subscription = PersistentReplicator.getRemoteCluster(subscription);
            }
            for (Topic topic : topicList) {
                if (topic instanceof PersistentTopic
                        && !pulsar().getBrokerService().isSystemTopic(TopicName.get(topic.getName()))) {
                    futures.add(((PersistentTopic) topic).clearBacklog(subscription));
                }
            }
        } else {
            for (Topic topic : topicList) {
                if (topic instanceof PersistentTopic
                        && !pulsar().getBrokerService().isSystemTopic(TopicName.get(topic.getName()))) {
                    futures.add(((PersistentTopic) topic).clearBacklog());
                }
            }
        }

        return FutureUtil.waitForAll(futures);
    }

    private CompletableFuture<Void> unsubscribeAsync(NamespaceName nsName, String bundleRange, String subscription) {

        List<Topic> topicList = pulsar().getBrokerService().getAllTopicsFromNamespaceBundle(nsName.toString(),
                nsName.toString() + "/" + bundleRange);
        List<CompletableFuture<Void>> futures = Lists.newArrayList();
        if (subscription.startsWith(pulsar().getConfiguration().getReplicatorPrefix())) {
            throw new RestException(Status.PRECONDITION_FAILED, "Cannot unsubscribeAsync a replication cursor");
        } else {
            for (Topic topic : topicList) {
                Subscription sub = topic.getSubscription(subscription);
                if (sub != null) {
                    futures.add(sub.delete());
                }
            }
        }
        return FutureUtil.waitForAll(futures);
    }

    protected BundlesData validateBundlesData(BundlesData initialBundles) {
        SortedSet<String> partitions = new TreeSet<String>();
        for (String partition : initialBundles.getBoundaries()) {
            Long partBoundary = Long.decode(partition);
            partitions.add(String.format("0x%08x", partBoundary));
        }
        if (partitions.size() != initialBundles.getBoundaries().size()) {
            if (log.isDebugEnabled()) {
                log.debug("Input bundles included repeated partition points. Ignored.");
            }
        }
        try {
            NamespaceBundleFactory.validateFullRange(partitions);
        } catch (IllegalArgumentException iae) {
            throw new RestException(Status.BAD_REQUEST, "Input bundles do not cover the whole hash range. first:"
                    + partitions.first() + ", last:" + partitions.last());
        }
        List<String> bundles = Lists.newArrayList();
        bundles.addAll(partitions);
        return BundlesData.builder()
                .boundaries(bundles)
                .numBundles(bundles.size() - 1)
                .build();
    }

    private void validatePolicies(NamespaceName ns, Policies policies) {
        if (ns.isV2() && policies.replication_clusters.isEmpty()) {
            // Default to local cluster
            policies.replication_clusters = Collections.singleton(config().getClusterName());
        }

        // Validate cluster names and permissions
        policies.replication_clusters.forEach(cluster -> validateClusterForTenant(ns.getTenant(), cluster));

        if (policies.message_ttl_in_seconds != null && policies.message_ttl_in_seconds < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for message TTL");
        }

        if (policies.bundles != null && policies.bundles.getNumBundles() > 0) {
            if (policies.bundles.getBoundaries() == null || policies.bundles.getBoundaries().size() == 0) {
                policies.bundles = getBundles(policies.bundles.getNumBundles());
            } else {
                policies.bundles = validateBundlesData(policies.bundles);
            }
        } else {
            int defaultNumberOfBundles = config().getDefaultNumberOfNamespaceBundles();
            policies.bundles = getBundles(defaultNumberOfBundles);
        }

        if (policies.persistence != null) {
            validatePersistencePolicies(policies.persistence);
        }

        if (policies.retention_policies != null) {
            validateRetentionPolicies(policies.retention_policies);
        }
    }

    private CompletableFuture<Void> validatePoliciesAsync(NamespaceName ns, Policies policies) {
        if (ns.isV2() && policies.replication_clusters.isEmpty()) {
            // Default to local cluster
            policies.replication_clusters = Collections.singleton(config().getClusterName());
        }

        // Validate cluster names and permissions
        return policies.replication_clusters.stream()
                    .map(cluster -> validateClusterForTenantAsync(ns.getTenant(), cluster))
                    .reduce(CompletableFuture.completedFuture(null), (a, b) -> a.thenCompose(ignore -> b))
            .thenAccept(__ -> {
                if (policies.message_ttl_in_seconds != null && policies.message_ttl_in_seconds < 0) {
                    throw new RestException(Status.PRECONDITION_FAILED, "Invalid value for message TTL");
                }

                if (policies.bundles != null && policies.bundles.getNumBundles() > 0) {
                    if (policies.bundles.getBoundaries() == null || policies.bundles.getBoundaries().size() == 0) {
                        policies.bundles = getBundles(policies.bundles.getNumBundles());
                    } else {
                        policies.bundles = validateBundlesData(policies.bundles);
                    }
                } else {
                    int defaultNumberOfBundles = config().getDefaultNumberOfNamespaceBundles();
                    policies.bundles = getBundles(defaultNumberOfBundles);
                }

                if (policies.persistence != null) {
                    validatePersistencePolicies(policies.persistence);
                }

                if (policies.retention_policies != null) {
                    validateRetentionPolicies(policies.retention_policies);
                }
            });
    }

    protected void validateRetentionPolicies(RetentionPolicies retention) {
        if (retention == null) {
            return;
        }
        checkArgument(retention.getRetentionSizeInMB() >= -1,
                "Invalid retention policy: size limit must be >= -1");
        checkArgument(retention.getRetentionTimeInMinutes() >= -1,
                "Invalid retention policy: time limit must be >= -1");
        checkArgument((retention.getRetentionTimeInMinutes() != 0 && retention.getRetentionSizeInMB() != 0)
                        || (retention.getRetentionTimeInMinutes() == 0 && retention.getRetentionSizeInMB() == 0),
                "Invalid retention policy: Setting a single time or size limit to 0 is invalid when "
                        + "one of the limits has a non-zero value. Use the value of -1 instead of 0 to ignore a "
                        + "specific limit. To disable retention both limits must be set to 0.");
    }

    protected CompletableFuture<Void> internalSetDeduplicationSnapshotInterval(Integer interval) {
        if (interval != null && interval < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "interval must be greater than or equal to 0");
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DEDUPLICATION_SNAPSHOT,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.deduplicationSnapshotIntervalSeconds = interval;
                        return policies;
                    });
                });
    }

    protected CompletableFuture<Void> internalSetMaxProducersPerTopic(Integer maxProducersPerTopic) {
        if (maxProducersPerTopic != null && maxProducersPerTopic < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxProducersPerTopic must be 0 or more");
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_PRODUCERS,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.max_producers_per_topic = maxProducersPerTopic;
                        return policies;
                    });
                });
    }

    protected CompletableFuture<Boolean> internalGetDeduplicationAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.DEDUPLICATION, PolicyOperation.READ)
                .thenCompose(__ -> getNamespacePoliciesAsync(namespaceName))
                .thenApply(policies -> policies.deduplicationEnabled);
    }

    protected CompletableFuture<Void> internalSetMaxConsumersPerTopicAsync(Integer maxConsumersPerTopic) {
        if (maxConsumersPerTopic != null && maxConsumersPerTopic < 0) {
            throw new RestException(Status.PRECONDITION_FAILED, "maxConsumersPerTopic must be 0 or more");
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_CONSUMERS,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.max_consumers_per_topic = maxConsumersPerTopic;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxConsumersPerSubscriptionAsync(Integer maxConsumersPerSubscription) {
        if (maxConsumersPerSubscription != null && maxConsumersPerSubscription < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxConsumersPerSubscription must be 0 or more");
        }

        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_CONSUMERS,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.max_consumers_per_subscription = maxConsumersPerSubscription;
                        return policies;
                    });
                });
    }

    protected CompletableFuture<Void> internalSetMaxUnackedMessagesPerConsumerAsync(Integer maxUnackedMessagesPerConsumer) {
        if (maxUnackedMessagesPerConsumer != null && maxUnackedMessagesPerConsumer < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedMessagesPerConsumer must be 0 or more");
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_UNACKED,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.max_unacked_messages_per_consumer = maxUnackedMessagesPerConsumer;
                        return policies;
                    });
                });

    }

    protected CompletableFuture<Void> internalSetMaxSubscriptionsPerTopic(Integer maxSubscriptionsPerTopic){
        if (maxSubscriptionsPerTopic != null && maxSubscriptionsPerTopic < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxSubscriptionsPerTopic must be 0 or more");
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_SUBSCRIPTIONS,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.max_subscriptions_per_topic = maxSubscriptionsPerTopic;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetMaxUnackedMessagesPerSubscriptionAsync(
            Integer maxUnackedMessagesPerSubscription) {
        if (maxUnackedMessagesPerSubscription != null && maxUnackedMessagesPerSubscription < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxUnackedMessagesPerSubscription must be 0 or more");
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_UNACKED,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.max_unacked_messages_per_subscription = maxUnackedMessagesPerSubscription;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetCompactionThreshold(Long newThreshold) {
        if (newThreshold != null && newThreshold < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "compactionThreshold must be 0 or more");
        }
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.COMPACTION,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.compaction_threshold = newThreshold;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetOffloadThresholdAsync(long newThreshold) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    if (policies.offload_policies == null) {
                        policies.offload_policies = new OffloadPoliciesImpl();
                    }
                    ((OffloadPoliciesImpl) policies.offload_policies)
                            .setManagedLedgerOffloadThresholdInBytes(newThreshold);
                    policies.offload_threshold = newThreshold;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetOffloadDeletionLagAsync(Long newDeletionLagMs) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    if (policies.offload_policies == null) {
                        policies.offload_policies = new OffloadPoliciesImpl();
                    }
                    ((OffloadPoliciesImpl) policies.offload_policies)
                            .setManagedLedgerOffloadDeletionLagInMillis(newDeletionLagMs);
                    policies.offload_deletion_lag_ms = newDeletionLagMs;
                    return policies;
                }));
    }

    @Deprecated
    protected SchemaAutoUpdateCompatibilityStrategy internalGetSchemaAutoUpdateCompatibilityStrategy() {
        validateNamespacePolicyOperation(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.READ);
        return getNamespacePolicies(namespaceName).schema_auto_update_compatibility_strategy;
    }

    @Deprecated
    protected CompletableFuture<Void> internalSetSchemaAutoUpdateCompatibilityStrategyAsync(
            SchemaAutoUpdateCompatibilityStrategy strategy) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.schema_auto_update_compatibility_strategy = strategy;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetSchemaCompatibilityStrategyAsync(SchemaCompatibilityStrategy strategy) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.schema_compatibility_strategy = strategy;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetSchemaValidationEnforced(boolean schemaValidationEnforced) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.schema_validation_enforced = schemaValidationEnforced;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalSetIsAllowAutoUpdateSchemaAsync(boolean isAllowAutoUpdateSchema) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SCHEMA_COMPATIBILITY_STRATEGY,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.is_allow_auto_update_schema = isAllowAutoUpdateSchema;
                        return policies;
                    });
                });
    }

    protected CompletableFuture<Void> internalSetSubscriptionTypesEnabledAsync(Set<SubscriptionType>
                                                                                       subscriptionTypesEnabled) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.SUBSCRIPTION_AUTH_MODE,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    Set<String> subTypes = new HashSet<>();
                    subscriptionTypesEnabled.forEach(subscriptionType -> subTypes.add(subscriptionType.name()));
                    return updatePoliciesAsync(namespaceName, policies -> {
                        policies.subscription_types_enabled = subTypes;
                        return policies;
                    });
                });
    }


    private <T> void mutatePolicy(Function<Policies, Policies> policyTransformation,
                                  Function<Policies, T> getter,
                                  String policyName) {
        try {
            MutableObject exception = new MutableObject(null);
            MutableObject policiesObj = new MutableObject(null);
            updatePolicies(namespaceName, policies -> {
                try {
                    policies = policyTransformation.apply(policies);
                } catch (Exception e) {
                    exception.setValue(e);
                }
                policiesObj.setValue(policies);
                return policies;
            });
            if (exception.getValue() != null) {
                throw (Exception) exception.getValue();
            }
            log.info("[{}] Successfully updated {} configuration: namespace={}, value={}", clientAppId(), policyName,
                    namespaceName, getter.apply((Policies) policiesObj.getValue()));
        } catch (RestException pfe) {
            throw pfe;
        } catch (Exception e) {
            log.error("[{}] Failed to update {} configuration for namespace {}",
                    clientAppId(), policyName, namespaceName, e);
            throw new RestException(e);
        }
    }

    protected CompletableFuture<Void> internalSetOffloadPoliciesAsync(OffloadPoliciesImpl offloadPolicies) {
        validateOffloadPolicies(offloadPolicies);
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    if (Objects.equals(offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis(),
                            OffloadPoliciesImpl.DEFAULT_OFFLOAD_DELETION_LAG_IN_MILLIS)) {
                        offloadPolicies.setManagedLedgerOffloadDeletionLagInMillis(policies.offload_deletion_lag_ms);
                    } else {
                        policies.offload_deletion_lag_ms = offloadPolicies.getManagedLedgerOffloadDeletionLagInMillis();
                    }
                    if (Objects.equals(offloadPolicies.getManagedLedgerOffloadThresholdInBytes(),
                            OffloadPoliciesImpl.DEFAULT_OFFLOAD_THRESHOLD_IN_BYTES)) {
                        offloadPolicies.setManagedLedgerOffloadThresholdInBytes(policies.offload_threshold);
                    } else {
                        policies.offload_threshold = offloadPolicies.getManagedLedgerOffloadThresholdInBytes();
                    }
                    policies.offload_policies = offloadPolicies;
                    return policies;
                }));
    }

    protected CompletableFuture<Void> internalRemoveOffloadPoliciesAsync() {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.OFFLOAD,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.offload_policies = null;
                    return policies;
                }));
    }

    private void validateOffloadPolicies(OffloadPoliciesImpl offloadPolicies) {
        if (offloadPolicies == null) {
            log.warn("[{}] Failed to update offload configuration for namespace {}: offloadPolicies is null",
                    clientAppId(), namespaceName);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "The offloadPolicies must be specified for namespace offload.");
        }
        if (!offloadPolicies.driverSupported()) {
            log.warn("[{}] Failed to update offload configuration for namespace {}: "
                            + "driver is not supported, support value: {}",
                    clientAppId(), namespaceName, OffloadPoliciesImpl.getSupportedDriverNames());
            throw new RestException(Status.PRECONDITION_FAILED,
                    "The driver is not supported, support value: " + OffloadPoliciesImpl.getSupportedDriverNames());
        }
        if (!offloadPolicies.bucketValid()) {
            log.warn("[{}] Failed to update offload configuration for namespace {}: bucket must be specified",
                    clientAppId(), namespaceName);
            throw new RestException(Status.PRECONDITION_FAILED,
                    "The bucket must be specified for namespace offload.");
        }
    }

   protected CompletableFuture<Void> internalSetMaxTopicsPerNamespaceAsync(Integer maxTopicsPerNamespace) {
        if (maxTopicsPerNamespace != null && maxTopicsPerNamespace < 0) {
            throw new RestException(Status.PRECONDITION_FAILED,
                    "maxTopicsPerNamespace must be 0 or more");
        }
       return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.MAX_TOPICS,
               PolicyOperation.WRITE)
               .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
               .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                   policies.max_topics_per_namespace = maxTopicsPerNamespace;
                   return policies;
               }));
   }

   protected CompletableFuture<Void> internalSetPropertyAsync(String key, String value, AsyncResponse asyncResponse) {
       return validatePoliciesReadOnlyAccessAsync()
               .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                   policies.properties.put(key, value);
                   return policies;
               }));
   }

   protected CompletableFuture<Void> internalSetPropertiesAsync(Map<String, String> properties, AsyncResponse asyncResponse) {
       return validatePoliciesReadOnlyAccessAsync()
               .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                   policies.properties.putAll(properties);
                   return policies;
               }));
   }

   protected CompletableFuture<Void> internalRemovePropertyAsync(String key) {
       return validatePoliciesReadOnlyAccessAsync()
               .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                   policies.properties.remove(key);
                   return policies;
               }));
   }

   protected CompletableFuture<Void> internalClearPropertiesAsync() {
       return validatePoliciesReadOnlyAccessAsync()
               .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                   policies.properties.clear();
                   return policies;
               }));
   }

   private CompletableFuture<Void> updatePoliciesAsync(NamespaceName ns, Function<Policies, Policies> updateFunction) {
       CompletableFuture<Void> result = new CompletableFuture<>();
       namespaceResources().setPoliciesAsync(ns, updateFunction)
           .thenAccept(v -> {
               log.info("[{}] Successfully updated the policies on namespace {}", clientAppId(), namespaceName);
               result.complete(null);
           })
           .exceptionally(ex -> {
               Throwable cause = FutureUtil.unwrapCompletionException(ex);
               if (cause instanceof NotFoundException) {
                   result.completeExceptionally(new RestException(Status.NOT_FOUND, "Namespace does not exist"));
               } else if (cause instanceof BadVersionException) {
                   log.warn("[{}] Failed to update the replication clusters on"
                                   + " namespace {} : concurrent modification", clientAppId(), namespaceName);
                   result.completeExceptionally(new RestException(Status.CONFLICT, "Concurrent modification"));
               } else {
                   log.error("[{}] Failed to update namespace policies {}", clientAppId(), namespaceName, cause);
                   result.completeExceptionally(new RestException(cause));
               }
               return null;
           });
       return result;
   }

   private void updatePolicies(NamespaceName ns, Function<Policies, Policies> updateFunction) {
       // Force to read the data s.t. the watch to the cache content is setup.
       try {
           updatePoliciesAsync(ns, updateFunction).get(namespaceResources().getOperationTimeoutSec(),
                   TimeUnit.SECONDS);
       } catch (Exception e) {
           Throwable cause = e.getCause();
           if (!(cause instanceof RestException)) {
               throw new RestException(cause);
           } else {
               throw (RestException) cause;
           }
       }
   }

    protected CompletableFuture<Void> internalSetNamespaceResourceGroupAsync(String rgName) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.RESOURCEGROUP,
                PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> {
                    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
                    if (rgName != null) {
                        future = resourceGroupResources().getResourceGroupAsync(rgName)
                                .thenAccept(optRes -> {
                                    if (optRes.isEmpty()) {
                                        throw new RestException(Status.PRECONDITION_FAILED,
                                                "ResourceGroup does not exist");
                                    }
                                });
                    }
                    return future;
                })
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.resource_group_name = rgName;
                    return policies;
                }));
    }

    protected void internalScanOffloadedLedgers(OffloaderObjectsScannerUtils.ScannerResultSink sink)
            throws Exception {
        log.info("internalScanOffloadedLedgers {}", namespaceName);
        validateNamespacePolicyOperation(namespaceName, PolicyName.OFFLOAD, PolicyOperation.READ);

        Policies policies = getNamespacePolicies(namespaceName);
        LedgerOffloader managedLedgerOffloader = pulsar()
                .getManagedLedgerOffloader(namespaceName, (OffloadPoliciesImpl) policies.offload_policies);

        String localClusterName = pulsar().getConfiguration().getClusterName();

        OffloaderObjectsScannerUtils.scanOffloadedLedgers(managedLedgerOffloader,
                localClusterName, pulsar().getManagedLedgerFactory(), sink);

    }

    protected CompletableFuture<Void> internalSetEntryFiltersPerTopicAsync(EntryFilters entryFilters) {
        return validateNamespacePolicyOperationAsync(namespaceName, PolicyName.ENTRY_FILTERS, PolicyOperation.WRITE)
                .thenCompose(__ -> validatePoliciesReadOnlyAccessAsync())
                .thenCompose(__ -> updatePoliciesAsync(namespaceName, policies -> {
                    policies.entryFilters = entryFilters;
                    return policies;
                }));
    }

    private static final Logger log = LoggerFactory.getLogger(NamespacesBase.class);
}
