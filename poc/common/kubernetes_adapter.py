from kubernetes import client, config

config.load_kube_config()

v1 = client.CoreV1Api()

namespace = "flink-jobs"
configmap_name = "flink-config-sl0004-l-g01-kafka"

new_routes_conf = """routes: [
  {
    source: "shared_kafka_001"
    topicName: "alertassignation"
    targetTable: "alertassignation"
    dataType: "alertassignation"
  },
  {
    source: "shared_kafka_002"
    topicName: "some_other_topic"
    targetTable: "some_other_table"
    dataType: "someothertype"
  }
]"""

patch = {
    "data": {
        "routes.conf": new_routes_conf
    }
}

v1.patch_namespaced_config_map(
    name=configmap_name,
    namespace=namespace,
    body=patch,
)