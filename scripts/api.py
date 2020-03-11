import sys

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
    print contexts
    print active_context
    if not contexts:
        logger.error("Cannot find any context in kubeconfig file.")
        sys.exit(0)
    logger.info("found and read existing kube context")
    logger.info("interacting with cluster {}".format(active_context['context']['cluster']))
    print active_context['context']['cluster']
    print(os.environ['KUBECONFIG'])
    kubeconfig =  os.getenv('KUBECONFIG')
    config.load_kube_config(kubeconfig)
    v1 = client.CoreV1Api()
    list_of_all_nodes = v1.list_node()
    node_resources = []
    for node_map in list_of_all_nodes.items:
        node_utilization = compute_node_utilization(v1, node_map)
        node_resources.append(node_utilization)
        # break after a node to iterate fasteri
        if len(node_utilization) > 1:
            break
    pod_cnt = sum([len(node) for node in node_resources])
    print ("pod count ", pod_cnt)
    print ("node_resources ",node_resources)

def normalize_measurement(value, res_type):
    if value == 0:
        return 0
    if res_type == 'cpu':
        # cpu conversion to millicpu
        if value[-1] == 'm': # if already in appropiated unit
            return float(value[:-1])
        else:
            return math.ceil(float(value) * 1000.0) # convert to millicpu
    elif res_type == "memory":
        # to ki
        conversion_table_i = {'Ei' : 2**50, 'Pi': 2**40, 'Ti' : 2**30, 'Gi' : 2**20, 'Mi' : 2**10, 'Ki' : 1}
        conversion_table = { 'E': 10**15, 'P':10**12, 'T': 10**9, 'G' : 10**6, 'M' : 10**3, 'K' : 1}
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
    #print node_map.metadata
    node_id = node_map.metadata.name
    print("node_name ", node_id)
    # now describe this node.
    try:
        node = v1.read_node(name=node_id)
    except kubernetes.client.rest.ApiException:
        # todo: add retries, etc
        return []
    #print("node_name ", node)
    '''
    field_selector = {
        'metadata.name': node_id,
        'metadata.labels.stack'='avingestmagna--1-0-464--dmf72t87',
    }
    '''
    field_selector = "spec.nodeName="+node_id
    #pod = v1.list_pod_for_all_namespaces(field_selector=field_selector)
    try:
        pods = v1.list_namespaced_pod("prod", field_selector=field_selector)
    except:
        # todo: better handle of errors
        return []
    #print("pod  ", pods)
    node_resources_table = []
    # fetch limits and requested resources from k8s
    for pod in pods.items:
        resources = []
        # go over the results
        #import pdb;pdb.set_trace()
        name, nodename = pod.metadata.name ,pod.spec.node_name
        for container in pod.spec.containers:
            cont_res_req = container.resources.requests
            cont_res_limit = container.resources.limits
            resources.append((
                 get_or_default(cont_res_req, 'cpu'),
                 get_or_default(cont_res_req, 'memory'),
                 get_or_default(cont_res_limit, 'cpu'),
                 get_or_default(cont_res_limit, 'memory'),
                 ))
        pod_resource_utilization = (node_id, name, resources)
        node_resources_table.append(pod_resource_utilization)
    return node_resources_table
if __name__ == "__main__":
    print(timeit.timeit(_k8s_setup, number = 1))