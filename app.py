from google.api_core import retry
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import json
import pandas as pd
from pytz import timezone
from google.cloud import bigquery
import os
import time

PUBSUB_PROJECT_ID = os.environ.get("PUBSUB_PROJECT_ID","signalscz-demo")
PUBSUB_SUBSCRIPTION_ID = os.environ.get("PUBSUB_SUBSCRIPTION_ID","sgtm-test--pull")
PUBSUB_TIMEOUT = int(os.environ.get("PUBSUB_TIMEOUT",60))
PUBSUB_MAX_MESSAGES = int(os.environ.get("PUBSUB_MAX_MESSAGES",1000))

BIGQUERY_TABLE_ID = os.environ.get("BIGQUERY_TABLE_ID","signalscz-demo.otto_test.pubsub_pull")

received_messages = []
def extract(project_id, subscription_id, timeout=10, max_messages=10):
    """Receives messages from a pull subscription with flow control."""
    # TODO(developer)
    # project_id = "your-project-id"
    # subscription_id = "your-subscription-id"
    # Number of seconds the subscriber should listen for messages
    # timeout = 5.0

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        #print(f"Received {message.data!r}.")
        message.ack()
        #print(f"Done processing the message {message.data!r}.")

        message_data = message.data.decode("utf-8")
        message_json = dict()
        try:
            message_json = json.loads(message_data)
        except json.JSONDecodeError as e:
            pass # Drop the message if it is not a valid json string
        message_json["publish_time"] = message.publish_time

        global received_messages
        received_messages.append(message_json)
                          

    # Limit the subscriber to only have ten outstanding messages at a time.
    flow_control = pubsub_v1.types.FlowControl(max_messages=max_messages)

    streaming_pull_future = subscriber.subscribe(
        subscription_path, callback=callback, await_callbacks_on_shutdown=True, flow_control=flow_control
    )
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.

def transform(dict_data):
    # Create a DataFrame from the messages
    df_base = pd.DataFrame(dict_data)
    # Convert times to Prague timezone
    df_base['datetime_prague'] = df_base['publish_time'].dt.tz_convert(timezone('Europe/Prague'))
    df_base['datetime_prague'] = df_base['datetime_prague'].dt.floor('min')
    # Convert times to strings
    df_base['date_prague'] = df_base['datetime_prague'].dt.strftime('%Y-%m-%d')
    df_base['datetime_prague'] = df_base['datetime_prague'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # 1 min statistics
    df_agg_1min = (df_base.sort_values('publish_time', ascending=True)
                   .groupby(['date_prague', 'datetime_prague'])
                   .agg('count')).reset_index()
    df_agg_1min.columns = ['date_prague', 'datetime_prague', 'count']


    return df_agg_1min

def load(rows_to_insert):
  # Construct a BigQuery client object.
  client = bigquery.Client()

  # Make an API request.
  errors = client.insert_rows_json(BIGQUERY_TABLE_ID,
                                   rows_to_insert,
                                   skip_invalid_rows=True,
                                   ignore_unknown_values=True)  
  if errors == []:
      #print("New rows have been added.")
      pass
  else:
      print("Encountered errors while inserting rows: {}".format(errors))

if __name__ == '__main__':
  start_time = time.time()
  extract(PUBSUB_PROJECT_ID, PUBSUB_SUBSCRIPTION_ID, PUBSUB_TIMEOUT, PUBSUB_MAX_MESSAGES)
  print(f"Extraction time: {time.time()-start_time} seconds")
  if received_messages:
    df_messages_agg = transform(received_messages)
    # Convert df to json
    rows_to_insert = df_messages_agg.to_dict(orient='records')
    load(rows_to_insert)
  print(f"Received messages: {len(received_messages)}")
