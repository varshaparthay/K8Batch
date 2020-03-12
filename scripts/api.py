import sys
import csv

import kubernetes
from kubernetes import client, config
import logging
import os
import math

logger = logging.getLogger()
import timeit


class ResourceReq(object):
    def __init__(self, cup_req, mem_req, cpu_limit, memory_limit):
        self.resource_vector = (cpu_req, mem_req, cpu_limit, memory_limit)


class Pod(object):
    def __init__(self, name, resources):
        self.name = name
        self.resources = resources


"""
Node contain this information
'status': {'addresses': [{'address': '10.162.145.103',
                           'type': 'InternalIP'},
                          {'address': 'ip-10-162-145-103.us-west-2.compute.internal',
                           'type': 'InternalDNS'},
                          {'address': 'ip-10-162-145-103.us-west-2.compute.internal',
                           'type': 'Hostname'}],
            'allocatable': {u'attachable-volumes-aws-ebs': '39',
                            u'cpu': '39830m',
                            u'ephemeral-storage': '493695146804',
                            u'hugepages-1Gi': '0',
                            u'hugepages-2Mi': '0',
                            u'memory': '154549576Ki',
                            u'pods': '234'},
            'capacity': {u'attachable-volumes-aws-ebs': '39',
                         u'cpu': '40',
                         u'ephemeral-storage': '536858604Ki',
                         u'hugepages-1Gi': '0',
                         u'hugepages-2Mi': '0',
                         u'memory': '165039432Ki',
                         u'pods': '234'},
"""


def _k8s_setup():
    contexts, active_context = config.list_kube_config_contexts()
    if not contexts:
        logger.error("Cannot find any context in kubeconfig file.")
        sys.exit(0)
    logger.info("found and read existing kube context")
    kubeconfig = os.getenv('KUBECONFIG')
    config.load_kube_config(kubeconfig)
    v1 = client.CoreV1Api()
    list_of_all_nodes = v1.list_node()
    node_resources = []
    resource_usage_per_pod = []
    # List of dictionaries
    for node_map in list_of_all_nodes.items:
        node_utilization, node_resource_percentage = compute_node_utilization(v1, node_map)
        node_resources.append(node_utilization)
        for item in node_resource_percentage:
            resource_usage_per_pod.append(item)
        # break after a node to iterate faster
        if len(node_utilization) > 0:
            break

    #export resource usage per pods to a csv file
    fields = ['pod_name', 'cpu_usage', 'memory_usage', 'cpu_cost', 'memory_cost']
    filename = "/tmp/node_resource_file.csv"
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerows(resource_usage_per_pod)

def normalize_measurement(value, res_type):
    if value == 0:
        return 0
    if res_type == 'cpu':
        # cpu conversion to millicpu
        if value[-1] == 'm':  # if already in appropriate unit
            return float(value[:-1])
        else:
            return math.ceil(float(value) * 1000.0)  # convert to millicpu
    elif res_type == "memory":
        # to ki
        conversion_table_i = {'Ei': 2 ** 50, 'Pi': 2 ** 40, 'Ti': 2 ** 30, 'Gi': 2 ** 20, 'Mi': 2 ** 10, 'Ki': 1}
        conversion_table = {'E': 10 ** 15, 'P': 10 ** 12, 'T': 10 ** 9, 'G': 10 ** 6, 'M': 10 ** 3, 'K': 1, 'm': 10 ** -6}
        conversion_ratio = 1
        numeric_value = 0
        if value[-1] == 'i':
            conversion_ratio = conversion_table_i[value[-2:]]
            numeric_value = float(value[:-2])
        else:
            conversion_ratio = conversion_table[value[-1]]
            numeric_value = float(value[:-1])
        return math.ceil(conversion_ratio * numeric_value)
    else:
        return value


def get_or_default(resource, res_type):
    if resource is None:
        return 0
    else:
        return normalize_measurement(resource.get(res_type, 0), res_type)


def compute_node_utilization(v1, node_map):
    # fetch request resources/limits from the node

    node_id = node_map.metadata.name
    print("Processing node ", node_id)
    total_memory_capacity_of_a_node = normalize_measurement(node_map.status.allocatable['memory'], "memory")
    total_cpu_capacity_of_a_node = normalize_measurement(node_map.status.allocatable['cpu'], "cpu")

    try:
        node = v1.read_node(name=node_id)
    except kubernetes.client.rest.ApiException:
        # todo: add retries, etc
        return []
    '''
    field_selector = {
        'metadata.name': node_id,
        'metadata.labels.stack'='avingestmagna--1-0-464--dmf72t87',
    }
    '''
    field_selector = "spec.nodeName=" + node_id
    try:
        pods = v1.list_namespaced_pod("prod", field_selector=field_selector)
    except:
        # todo: better handle of errors
        return []

    # fetch limits and requested resources from k8s
    node_resources_table = []
    node_resource_percentages = []  # type: List[Dict[str, float]]
    total_cpu_cost_of_pod, total_memory_cost_of_pod = 0,0
    for pod in pods.items:
        total_memory_requested_by_pod, total_cpu_requested_by_pod = 0, 0
        resources = []
        percentages = {}
        name = pod.metadata.name

        ## For every container in a pod
        for container in pod.spec.containers:
            cont_res_req = container.resources.requests
            cont_res_limit = container.resources.limits

            cpu_requested_by_pod = get_or_default(cont_res_req, 'cpu')
            memory_requested_by_pod = get_or_default(cont_res_req, 'memory')
            cpu_limits_of_pod = get_or_default(cont_res_limit, 'cpu')
            memory_limits_of_pod = get_or_default(cont_res_limit, 'memory')

            ## Compute total memory and cpu requested by all containers in a pod
            total_memory_requested_by_pod += memory_requested_by_pod
            total_cpu_requested_by_pod += cpu_requested_by_pod

            ## Compute total cost of cpu and memory for all pods in a node
            total_cpu_cost_of_pod += cpu_requested_by_pod
            total_memory_cost_of_pod += memory_requested_by_pod

            resources.append((
                cpu_requested_by_pod,
                memory_requested_by_pod,
                cpu_limits_of_pod,
                memory_limits_of_pod,
            ))

        pod_resource_utilization = (node_id, name, resources)
        node_resources_table.append(pod_resource_utilization)
        percentages['pod_name'] = name
        percentages['memory_usage'] = (total_memory_requested_by_pod / float(total_memory_capacity_of_a_node)) * 100
        percentages['cpu_usage'] = (total_cpu_requested_by_pod / float(total_cpu_capacity_of_a_node)) * 100
        node_resource_percentages.append(percentages)

    '''
    Compute cost per pod now, 
    this is not the most effective way to compute cost, but taking a first stab
    '''

    for pod in pods.items:
        total_memory_requested_by_pod, total_cpu_requested_by_pod = 0, 0
        name = pod.metadata.name
        for container in pod.spec.containers:
            cont_res_req = container.resources.requests
            cpu_requested_by_pod = get_or_default(cont_res_req, 'cpu')
            memory_requested_by_pod = get_or_default(cont_res_req, 'memory')
            ## Compute total memory and cpu requested by all containers in a pod
            total_memory_requested_by_pod += memory_requested_by_pod
            total_cpu_requested_by_pod += cpu_requested_by_pod

        # find the right pod and update dictionary
        for resource in node_resource_percentages:
            if str(resource['pod_name']) == str(name):
                resource['cpu_cost'] = (total_cpu_requested_by_pod/total_cpu_cost_of_pod) * 100
                resource['memory_cost'] = (total_memory_requested_by_pod/total_memory_cost_of_pod) * 100

    return node_resources_table, node_resource_percentages


if __name__ == "__main__":
    print("time taken to compute 1 pod resources", timeit.timeit(_k8s_setup, number=1))
