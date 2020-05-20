import boto3
import time
import calendar


def lambda_handler(event, context):
    client = boto3.client('quicksight',  region_name="region")
    res = client.list_data_sets(AwsAccountId='account-id')

    # filter out your datasets using a prefix. All my datasets have chicago_crimes as their prefix
    datasets_ids = [summary["DataSetId"] for summary in res["DataSetSummaries"] if "chicago_crimes" in summary["Name"]]
    ingestion_ids = []

    for dataset_id in datasets_ids:
        try:
            ingestion_id = str(calendar.timegm(time.gmtime()))
            client.create_ingestion(DataSetId=dataset_id, IngestionId=ingestion_id,
                                                 AwsAccountId='account-id')
            ingestion_ids.append(ingestion_id)
        except Exception as e:
            print(e)
            pass

    for ingestion_id, dataset_id in zip(ingestion_ids, datasets_ids):
        while True:
            response = client.describe_ingestion(DataSetId=dataset_id,
                                                 IngestionId=ingestion_id,
                                                 AwsAccountId='account-id')
            if response['Ingestion']['IngestionStatus'] in ('INITIALIZED', 'QUEUED', 'RUNNING'):
                time.sleep(5)     #change sleep time according to your dataset size
            elif response['Ingestion']['IngestionStatus'] == 'COMPLETED':
                print("refresh completed. RowsIngested {0}, RowsDropped {1}, IngestionTimeInSeconds {2}, IngestionSizeInBytes {3}".format(
                    response['Ingestion']['RowInfo']['RowsIngested'],
                    response['Ingestion']['RowInfo']['RowsDropped'],
                    response['Ingestion']['IngestionTimeInSeconds'],
                    response['Ingestion']['IngestionSizeInBytes']))
                break
            else:
                print("refresh failed for {0}! - status {1}".format(dataset_id, response['Ingestion']['IngestionStatus']))
                break
