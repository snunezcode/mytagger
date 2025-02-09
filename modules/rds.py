import json
import boto3
from typing import List, Dict, Tuple
from botocore.exceptions import OperationNotPageableError

def discovery(self, session, account_id, region, service, service_type, logger):

    resource_configs = {
            'DBCluster': {
                'method': 'describe_db_clusters',
                'key': 'DBClusters',
                'id_field': 'DBClusterIdentifier',
                'date_field': 'ClusterCreateTime',
                'nested': False,
                'arn_format': 'arn:aws:rds:{region}:{account_id}:cluster:{resource_id}',
                'tag_list_field': 'TagList'
            },
            'DBClusterSnapshot': {
                'method': 'describe_db_cluster_snapshots',
                'key': 'DBClusterSnapshots',
                'id_field': 'DBClusterSnapshotIdentifier',
                'date_field': 'SnapshotCreateTime',
                'nested': False,
                'arn_format': 'arn:aws:rds:{region}:{account_id}:cluster-snapshot:{resource_id}',
                'tag_list_field': 'TagList'
            },
            'DBInstance': {
                'method': 'describe_db_instances',
                'key': 'DBInstances',
                'id_field': 'DBInstanceIdentifier',
                'date_field': 'InstanceCreateTime',
                'nested': False,
                'arn_format': 'arn:aws:rds:{region}:{account_id}:db:{resource_id}',
                'tag_list_field': 'TagList'
            },
            'DBSnapshot': {
                'method': 'describe_db_snapshots',
                'key': 'DBSnapshots',
                'id_field': 'DBSnapshotIdentifier',
                'date_field': 'SnapshotCreateTime',
                'nested': False,
                'arn_format': 'arn:aws:rds:{region}:{account_id}:snapshot:{resource_id}',
                'tag_list_field': 'TagList'
            },
            'SubnetGroup': {
                'method': 'describe_db_subnet_groups',
                'key': 'DBSubnetGroups',
                'id_field': 'DBSubnetGroupName',
                'date_field': None,
                'nested': False,
                'arn_format': 'arn:aws:rds:{region}:{account_id}:subgrp:{resource_id}',
                'tag_list_field': None
            },
            'OptionGroup': {
                'method': 'describe_option_groups',
                'key': 'OptionGroupsList',
                'id_field': 'OptionGroupName',
                'date_field': None,
                'nested': False,
                'arn_format': 'arn:aws:rds:{region}:{account_id}:og:{resource_id}',
                'tag_list_field': None
            },
            'DBClusterParameterGroup': {
                'method': 'describe_db_cluster_parameter_groups',
                'key': 'DBClusterParameterGroups',
                'id_field': 'DBClusterParameterGroupName',
                'date_field': None,
                'nested': False,
                'arn_format': 'arn:aws:rds:{region}:{account_id}:cluster-pg:{resource_id}',
                'tag_list_field': None
            },
            'DBParameterGroup': {
                'method': 'describe_db_parameter_groups',
                'key': 'DBParameterGroups',
                'id_field': 'DBParameterGroupName',
                'date_field': None,
                'nested': False,
                'arn_format': 'arn:aws:rds:{region}:{account_id}:pg:{resource_id}',
                'tag_list_field': None
            },
            'DBSecurityGroup': {
                'method': 'describe_db_security_groups',
                'key': 'DBSecurityGroups',
                'id_field': 'DBSecurityGroupName',
                'date_field': None,
                'nested': False,
                'arn_format': 'arn:aws:rds:{region}:{account_id}:secgrp:{resource_id}',
                'tag_list_field': None
            }
        }

    if service_type not in resource_configs:
            raise ValueError(f"Unsupported service type: {service_type}")

    return _discover_resources(session, account_id, region, service, service_type, resource_configs[service_type], logger)


def _discover_resources(session, account_id, region, service, service_type, config, logger):   

    client = session.client(service, region_name=region)
    resources = []

    method = getattr(client, config['method'])
    params = {}

    try:
        paginator = client.get_paginator(config['method'])
        response_iterator = paginator.paginate(**params)
    except OperationNotPageableError:
        # If the operation is not pageable, call the method directly
        response_iterator = [method(**params)]

    for page in response_iterator:
        items = page[config['key']]

        for item in items:

            resource_id = item[config['id_field']]            
            creation_date = item.get(config['date_field']) if config['date_field'] else None
            
            # Construct the ARN
            arn = config['arn_format'].format(
                    region=region,
                    account_id=account_id,
                    resource_id=resource_id
            )

            # Handle tags based on the resource type
            if config['tag_list_field']:
                resource_tags = {tag['Key']: tag['Value'] for tag in item.get(config['tag_list_field'], [])}
            else:                
                resource_tags = _get_tags_for_resource(client, arn)

            name_tag = resource_tags.get('Name', resource_id)

            resources.append({
                    "seq": 0,
                    "account_id": account_id,
                    "region": region,
                    "service" : service,
                    "resource_type": service_type,
                    "resource_id": resource_id,
                    "name" : name_tag,
                    "creation_date": creation_date,
                    "tags": resource_tags,
                    "tags_number" : len(resource_tags),
                    "metadata" : item,
                    "arn" : arn
                })

    return resources


def _get_tags_for_resource(client, arn):
        try:
            response = client.list_tags_for_resource(ResourceName=arn)
            return {tag['Key']: tag['Value'] for tag in response.get('TagList', [])}
        except Exception as e:            
            return {}


####----| Tagging method
def tagging(account_id, region, service, client, resources, tags_string, tags_action, logger):
    
    logger.info(f'Discovery # Account : {account_id}, Region : {region}, Service : {service}')
    
    results = []    
    tags = parse_tags(tags_string)

    if tags_action == 2:        
        tags = [ item['Key'] for item in tags]

    for resource in resources:            
        try:            
         
            if tags_action == 1:               
                client.add_tags_to_resource(
                    ResourceName=resource.arn,
                    Tags=tags
                )
            elif tags_action == 2:
                client.remove_tags_from_resource(
                    ResourceName=resource.arn,
                    TagKeys=tags
                )                       
            results.append({
                'account_id': account_id,
                'region': region,
                'service': service,
                'identifier': resource.identifier,
                'status': 'success'
            })
            
        except Exception as e:
            logger.error(f"Error processing batch for {service} in {account_id}/{region}:{resource.identifier} # {str(e)}")
            
            results.append({
                'account_id': account_id,
                'region': region,
                'service': service,
                'identifier': resource.identifier,
                'status': 'error',
                'error': str(e)
            })    
    
    return results


####----| Parse method
def parse_tags(tags_string: str) -> List[Dict[str, str]]:
        tags = []
        for tag_pair in tags_string.split(','):
            key, value = tag_pair.split(':')
            tags.append({
                'Key': key.strip(),
                'Value': value.strip()
            })
        return tags
