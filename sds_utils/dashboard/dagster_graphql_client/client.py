from typing import Any, Optional, Union

from .all_asset_definitions import AllAssetDefinitions
from .asset_partition_history import AssetPartitionHistory
from .asset_partition_pair_states import AssetPartitionPairStates
from .asset_partition_state import AssetPartitionState
from .asset_partition_states import AssetPartitionStates
from .asset_records import AssetRecords
from .asset_status import AssetStatus
from .backfill_details import BackfillDetails
from .backfill_runs import BackfillRuns
from .backfills import Backfills
from .base_client import BaseClient
from .base_model import UNSET, UnsetType
from .input_types import AssetKeyInput, BulkActionsFilter
from .partition_run_events import PartitionRunEvents
from .partition_runs import PartitionRuns
from .recent_asset_activity import RecentAssetActivity
from .recent_failed_runs import RecentFailedRuns
from .run_asset_activity import RunAssetActivity


def gql(q: str) -> str:
    return q


class DagsterGraphQLClient(BaseClient):
    def all_asset_definitions(self, **kwargs: Any) -> AllAssetDefinitions:
        query = gql("""
            query AllAssetDefinitions {
              assetNodes {
                assetKey {
                  path
                }
                isPartitioned
                partitionKeys
              }
            }
            """)
        variables: dict[str, object] = {}
        response = self.execute(
            query=query,
            operation_name="AllAssetDefinitions",
            variables=variables,
            **kwargs,
        )
        data = self.get_data(response)
        return AllAssetDefinitions.model_validate(data)

    def asset_partition_history(
        self,
        asset_key: AssetKeyInput,
        limit: int,
        partitions: Union[Optional[list[str]], UnsetType] = UNSET,
        cursor: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> AssetPartitionHistory:
        query = gql("""
            query AssetPartitionHistory($assetKey: AssetKeyInput!, $partitions: [String!], $cursor: String, $limit: Int!) {
              assetOrError(assetKey: $assetKey) {
                __typename
                ... on Asset {
                  assetEventHistory(
                    partitions: $partitions
                    eventTypeSelectors: [MATERIALIZATION, FAILED_TO_MATERIALIZE, OBSERVATION]
                    cursor: $cursor
                    limit: $limit
                  ) {
                    results {
                      __typename
                      ... on FailedToMaterializeEvent {
                        runId
                        stepKey
                        partition
                        timestamp
                        materializationFailureType
                        materializationFailureReason
                        metadataEntries {
                          __typename
                          label
                          ... on TextMetadataEntry {
                            text
                          }
                        }
                      }
                      ... on MaterializationEvent {
                        runId
                        stepKey
                        partition
                        timestamp
                        metadataEntries {
                          __typename
                          label
                          ... on TextMetadataEntry {
                            text
                          }
                        }
                      }
                      ... on ObservationEvent {
                        runId
                        stepKey
                        partition
                        timestamp
                        metadataEntries {
                          __typename
                          label
                          ... on TextMetadataEntry {
                            text
                          }
                        }
                      }
                    }
                    cursor
                  }
                }
                ... on AssetNotFoundError {
                  message
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "assetKey": asset_key,
            "partitions": partitions,
            "cursor": cursor,
            "limit": limit,
        }
        response = self.execute(
            query=query,
            operation_name="AssetPartitionHistory",
            variables=variables,
            **kwargs,
        )
        data = self.get_data(response)
        return AssetPartitionHistory.model_validate(data)

    def asset_partition_pair_states(
        self,
        asset_keys: list[AssetKeyInput],
        partitions: list[str],
        partition: str,
        **kwargs: Any,
    ) -> AssetPartitionPairStates:
        query = gql("""
            query AssetPartitionPairStates($assetKeys: [AssetKeyInput!]!, $partitions: [String!]!, $partition: String!) {
              assetNodes(assetKeys: $assetKeys) {
                assetKey {
                  path
                }
                latestMaterializationByPartition(partitions: $partitions) {
                  runId
                  timestamp
                  partition
                  assetKey {
                    path
                  }
                }
                latestRunForPartition(partition: $partition) {
                  runId
                  status
                  updateTime
                  assetSelection {
                    path
                  }
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "assetKeys": asset_keys,
            "partitions": partitions,
            "partition": partition,
        }
        response = self.execute(
            query=query,
            operation_name="AssetPartitionPairStates",
            variables=variables,
            **kwargs,
        )
        data = self.get_data(response)
        return AssetPartitionPairStates.model_validate(data)

    def asset_partition_state(
        self, asset_key: AssetKeyInput, **kwargs: Any
    ) -> AssetPartitionState:
        query = gql("""
            query AssetPartitionState($assetKey: AssetKeyInput!) {
              assetNodeOrError(assetKey: $assetKey) {
                __typename
                ... on AssetNode {
                  assetKey {
                    path
                  }
                  partitionKeys
                  partitionDefinition {
                    dimensionTypes {
                      name
                      isPrimaryDimension
                    }
                  }
                  partitionKeysByDimension {
                    name
                    partitionKeys
                  }
                  assetPartitionStatuses {
                    __typename
                    ... on DefaultPartitionStatuses {
                      materializedPartitions
                      materializingPartitions
                      failedPartitions
                      unmaterializedPartitions
                    }
                    ... on TimePartitionStatuses {
                      ranges {
                        startKey
                        endKey
                        status
                      }
                    }
                    ... on MultiPartitionStatuses {
                      primaryDimensionName
                      ranges {
                        primaryDimStartKey
                        primaryDimEndKey
                        secondaryDim {
                          __typename
                          ... on DefaultPartitionStatuses {
                            materializedPartitions
                            materializingPartitions
                            failedPartitions
                            unmaterializedPartitions
                          }
                          ... on TimePartitionStatuses {
                            ranges {
                              startKey
                              endKey
                              status
                            }
                          }
                        }
                      }
                    }
                  }
                }
                ... on AssetNotFoundError {
                  message
                }
              }
            }
            """)
        variables: dict[str, object] = {"assetKey": asset_key}
        response = self.execute(
            query=query,
            operation_name="AssetPartitionState",
            variables=variables,
            **kwargs,
        )
        data = self.get_data(response)
        return AssetPartitionState.model_validate(data)

    def asset_partition_states(
        self, asset_keys: list[AssetKeyInput], **kwargs: Any
    ) -> AssetPartitionStates:
        query = gql("""
            query AssetPartitionStates($assetKeys: [AssetKeyInput!]!) {
              assetNodes(assetKeys: $assetKeys) {
                assetKey {
                  path
                }
                partitionKeys
                partitionDefinition {
                  dimensionTypes {
                    name
                    isPrimaryDimension
                  }
                }
                partitionKeysByDimension {
                  name
                  partitionKeys
                }
                assetPartitionStatuses {
                  __typename
                  ... on DefaultPartitionStatuses {
                    materializedPartitions
                    materializingPartitions
                    failedPartitions
                    unmaterializedPartitions
                  }
                  ... on TimePartitionStatuses {
                    ranges {
                      startKey
                      endKey
                      status
                    }
                  }
                  ... on MultiPartitionStatuses {
                    primaryDimensionName
                    ranges {
                      primaryDimStartKey
                      primaryDimEndKey
                      secondaryDim {
                        __typename
                        ... on DefaultPartitionStatuses {
                          materializedPartitions
                          materializingPartitions
                          failedPartitions
                          unmaterializedPartitions
                        }
                        ... on TimePartitionStatuses {
                          ranges {
                            startKey
                            endKey
                            status
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """)
        variables: dict[str, object] = {"assetKeys": asset_keys}
        response = self.execute(
            query=query,
            operation_name="AssetPartitionStates",
            variables=variables,
            **kwargs,
        )
        data = self.get_data(response)
        return AssetPartitionStates.model_validate(data)

    def asset_records(
        self, limit: int, cursor: Union[Optional[str], UnsetType] = UNSET, **kwargs: Any
    ) -> AssetRecords:
        query = gql("""
            query AssetRecords($cursor: String, $limit: Int!) {
              assetRecordsOrError(cursor: $cursor, limit: $limit) {
                __typename
                ... on AssetRecordConnection {
                  assets {
                    key {
                      path
                    }
                  }
                  cursor
                }
                ... on PythonError {
                  message
                  stack
                }
              }
            }
            """)
        variables: dict[str, object] = {"cursor": cursor, "limit": limit}
        response = self.execute(
            query=query, operation_name="AssetRecords", variables=variables, **kwargs
        )
        data = self.get_data(response)
        return AssetRecords.model_validate(data)

    def asset_status(
        self, asset_keys: list[AssetKeyInput], **kwargs: Any
    ) -> AssetStatus:
        query = gql("""
            query AssetStatus($assetKeys: [AssetKeyInput!]!) {
              assetNodes(assetKeys: $assetKeys, loadMaterializations: true) {
                assetKey {
                  path
                }
                assetMaterializations(limit: 1) {
                  timestamp
                  runId
                }
                partitionStats {
                  numPartitions
                  numMaterialized
                  numFailed
                  numMaterializing
                }
              }
            }
            """)
        variables: dict[str, object] = {"assetKeys": asset_keys}
        response = self.execute(
            query=query, operation_name="AssetStatus", variables=variables, **kwargs
        )
        data = self.get_data(response)
        return AssetStatus.model_validate(data)

    def backfills(
        self,
        limit: int,
        cursor: Union[Optional[str], UnsetType] = UNSET,
        filters: Union[Optional[BulkActionsFilter], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> Backfills:
        query = gql("""
            query Backfills($limit: Int!, $cursor: String, $filters: BulkActionsFilter) {
              partitionBackfillsOrError(limit: $limit, cursor: $cursor, filters: $filters) {
                __typename
                ... on PartitionBackfills {
                  results {
                    id
                    status
                    title
                    description
                    creationTime
                    endTime
                    numPartitions
                    isAssetBackfill
                  }
                }
                ... on PythonError {
                  message
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "limit": limit,
            "cursor": cursor,
            "filters": filters,
        }
        response = self.execute(
            query=query, operation_name="Backfills", variables=variables, **kwargs
        )
        data = self.get_data(response)
        return Backfills.model_validate(data)

    def backfill_details(self, backfill_id: str, **kwargs: Any) -> BackfillDetails:
        query = gql("""
            query BackfillDetails($backfillId: String!) {
              partitionBackfillOrError(backfillId: $backfillId) {
                __typename
                ... on PartitionBackfill {
                  id
                  status
                  title
                  description
                  creationTime
                  endTime
                  numPartitions
                  partitionNames
                  isAssetBackfill
                  assetBackfillData {
                    assetBackfillStatuses {
                      __typename
                      ... on AssetPartitionsStatusCounts {
                        assetKey {
                          path
                        }
                        numPartitionsTargeted
                        numPartitionsInProgress
                        numPartitionsMaterialized
                        numPartitionsFailed
                      }
                      ... on UnpartitionedAssetStatus {
                        assetKey {
                          path
                        }
                        inProgress
                        materialized
                        failed
                      }
                    }
                  }
                }
                ... on BackfillNotFoundError {
                  message
                }
                ... on PythonError {
                  message
                }
              }
            }
            """)
        variables: dict[str, object] = {"backfillId": backfill_id}
        response = self.execute(
            query=query, operation_name="BackfillDetails", variables=variables, **kwargs
        )
        data = self.get_data(response)
        return BackfillDetails.model_validate(data)

    def backfill_runs(
        self, backfill_id: str, run_limit: int, **kwargs: Any
    ) -> BackfillRuns:
        query = gql("""
            query BackfillRuns($backfillId: String!, $runLimit: Int!) {
              partitionBackfillOrError(backfillId: $backfillId) {
                __typename
                ... on PartitionBackfill {
                  id
                  runs(limit: $runLimit) {
                    runId
                    status
                    assetSelection {
                      path
                    }
                    tags {
                      key
                      value
                    }
                  }
                }
                ... on BackfillNotFoundError {
                  message
                }
                ... on PythonError {
                  message
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "backfillId": backfill_id,
            "runLimit": run_limit,
        }
        response = self.execute(
            query=query, operation_name="BackfillRuns", variables=variables, **kwargs
        )
        data = self.get_data(response)
        return BackfillRuns.model_validate(data)

    def partition_run_events(
        self,
        event_limit: int,
        run_ids: Union[Optional[list[str]], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> PartitionRunEvents:
        query = gql("""
            query PartitionRunEvents($runIds: [String!], $eventLimit: Int!) {
              runsOrError(filter: {runIds: $runIds}) {
                __typename
                ... on Runs {
                  results {
                    runId
                    eventConnection(limit: $eventLimit) {
                      events {
                        __typename
                        ... on MaterializationEvent {
                          runId
                          stepKey
                          timestamp
                          partition
                          assetKey {
                            path
                          }
                        }
                        ... on FailedToMaterializeEvent {
                          runId
                          stepKey
                          timestamp
                          partition
                          assetKey {
                            path
                          }
                          materializationFailureType
                          materializationFailureReason
                          metadataEntries {
                            __typename
                            label
                            ... on TextMetadataEntry {
                              text
                            }
                          }
                        }
                        ... on ObservationEvent {
                          runId
                          stepKey
                          timestamp
                          partition
                          assetKey {
                            path
                          }
                          metadataEntries {
                            __typename
                            label
                            ... on TextMetadataEntry {
                              text
                            }
                          }
                        }
                        ... on AssetMaterializationPlannedEvent {
                          timestamp
                          assetKey {
                            path
                          }
                        }
                        ... on RunFailureEvent {
                          runId
                          stepKey
                          timestamp
                        }
                        ... on LogMessageEvent {
                          runId
                          stepKey
                          timestamp
                          message
                        }
                      }
                      cursor
                      hasMore
                    }
                  }
                }
                ... on InvalidPipelineRunsFilterError {
                  message
                }
                ... on PythonError {
                  message
                  stack
                }
              }
            }
            """)
        variables: dict[str, object] = {"runIds": run_ids, "eventLimit": event_limit}
        response = self.execute(
            query=query,
            operation_name="PartitionRunEvents",
            variables=variables,
            **kwargs,
        )
        data = self.get_data(response)
        return PartitionRunEvents.model_validate(data)

    def partition_runs(
        self,
        partition: str,
        limit: int,
        cursor: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> PartitionRuns:
        query = gql("""
            query PartitionRuns($partition: String!, $cursor: String, $limit: Int!) {
              runsOrError(
                filter: {tags: [{key: "dagster/partition", value: $partition}]}
                cursor: $cursor
                limit: $limit
              ) {
                __typename
                ... on Runs {
                  results {
                    runId
                    status
                    updateTime
                    assetSelection {
                      path
                    }
                  }
                }
                ... on InvalidPipelineRunsFilterError {
                  message
                }
                ... on PythonError {
                  message
                  stack
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "partition": partition,
            "cursor": cursor,
            "limit": limit,
        }
        response = self.execute(
            query=query, operation_name="PartitionRuns", variables=variables, **kwargs
        )
        data = self.get_data(response)
        return PartitionRuns.model_validate(data)

    def recent_asset_activity(
        self,
        since: float,
        until: float,
        limit: int,
        cursor: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> RecentAssetActivity:
        query = gql("""
            query RecentAssetActivity($since: Float!, $until: Float!, $cursor: String, $limit: Int!) {
              runsOrError(
                filter: {updatedAfter: $since, updatedBefore: $until}
                cursor: $cursor
                limit: $limit
              ) {
                __typename
                ... on Runs {
                  results {
                    runId
                    status
                    updateTime
                    tags {
                      key
                      value
                    }
                    assetSelection {
                      path
                    }
                  }
                }
                ... on InvalidPipelineRunsFilterError {
                  message
                }
                ... on PythonError {
                  message
                  stack
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "since": since,
            "until": until,
            "cursor": cursor,
            "limit": limit,
        }
        response = self.execute(
            query=query,
            operation_name="RecentAssetActivity",
            variables=variables,
            **kwargs,
        )
        data = self.get_data(response)
        return RecentAssetActivity.model_validate(data)

    def recent_failed_runs(
        self,
        since: float,
        limit: int,
        cursor: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> RecentFailedRuns:
        query = gql("""
            query RecentFailedRuns($since: Float!, $cursor: String, $limit: Int!) {
              runsOrError(
                filter: {statuses: [FAILURE], updatedAfter: $since}
                cursor: $cursor
                limit: $limit
              ) {
                __typename
                ... on Runs {
                  results {
                    runId
                    jobName
                    status
                    startTime
                    endTime
                    updateTime
                  }
                }
                ... on InvalidPipelineRunsFilterError {
                  message
                }
                ... on PythonError {
                  message
                  stack
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "since": since,
            "cursor": cursor,
            "limit": limit,
        }
        response = self.execute(
            query=query,
            operation_name="RecentFailedRuns",
            variables=variables,
            **kwargs,
        )
        data = self.get_data(response)
        return RecentFailedRuns.model_validate(data)

    def run_asset_activity(
        self,
        run_id: str,
        limit: int,
        cursor: Union[Optional[str], UnsetType] = UNSET,
        **kwargs: Any,
    ) -> RunAssetActivity:
        query = gql("""
            query RunAssetActivity($runId: ID!, $cursor: String, $limit: Int!) {
              runOrError(runId: $runId) {
                __typename
                ... on Run {
                  eventConnection(afterCursor: $cursor, limit: $limit) {
                    events {
                      __typename
                      ... on MaterializationEvent {
                        runId
                        stepKey
                        timestamp
                        partition
                        assetKey {
                          path
                        }
                      }
                      ... on FailedToMaterializeEvent {
                        runId
                        stepKey
                        timestamp
                        partition
                        assetKey {
                          path
                        }
                        materializationFailureType
                        materializationFailureReason
                        metadataEntries {
                          __typename
                          label
                          ... on TextMetadataEntry {
                            text
                          }
                        }
                      }
                      ... on ObservationEvent {
                        runId
                        stepKey
                        timestamp
                        partition
                        assetKey {
                          path
                        }
                        metadataEntries {
                          __typename
                          label
                          ... on TextMetadataEntry {
                            text
                          }
                        }
                      }
                      ... on AssetMaterializationPlannedEvent {
                        timestamp
                        assetKey {
                          path
                        }
                      }
                      ... on RunFailureEvent {
                        runId
                        stepKey
                        timestamp
                      }
                      ... on LogMessageEvent {
                        runId
                        stepKey
                        timestamp
                        message
                      }
                    }
                    cursor
                    hasMore
                  }
                }
                ... on RunNotFoundError {
                  message
                }
                ... on PythonError {
                  message
                  stack
                }
              }
            }
            """)
        variables: dict[str, object] = {
            "runId": run_id,
            "cursor": cursor,
            "limit": limit,
        }
        response = self.execute(
            query=query,
            operation_name="RunAssetActivity",
            variables=variables,
            **kwargs,
        )
        data = self.get_data(response)
        return RunAssetActivity.model_validate(data)
