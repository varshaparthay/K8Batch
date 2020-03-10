import sys

from kubernetes import client, config
import logging
import os
logger = logging.getLogger()

def _k8s_setup():
    print "in K8 setup"
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
    for node_map in list_of_all_nodes.items:
        #print node_map.metadata
        node_id = node_map.metadata.name
        print("node_name ", node_id)
        # now describe this node.
        node = v1.read_node(name=node_id)
        #print("node_name ", node)
        '''
        field_selector = {
            'metadata.name': node_id
        }
        '''
        field_selector = "spec.nodeName="+node_id
        pod = v1.list_namespaced_pod("pdx", field_selector=field_selector)
        print("pod  ", pod)


if __name__ == "__main__":
    _k8s_setup()
