import pulumi
import pulumi_aws as aws


# ============================================================
# CONFIGURACIÓN GENERAL DEL PROYECTO RETAIL TPC-DS + EMR
# ============================================================

persistent_bucket_name: str = "bigdata-russell-academy"

tpcds_s3_data_path: str = f"s3://{persistent_bucket_name}/data/"
tpcds_s3_warehouse_path: str = f"s3://{persistent_bucket_name}/warehouse/tpcds/"
emr_logs_uri: str = f"s3://{persistent_bucket_name}/logs/emr-retail-tpcds/"

key_name: str = "emr-hive-lab-key"


# ============================================================
# SECURITY GROUP PARA SSH
# ============================================================

master_security_group = aws.ec2.SecurityGroup(
    resource_name="retail-tpcds-emr-master-ssh-sg",
    description="Allow SSH access to EMR primary node",

    ingress=[
        aws.ec2.SecurityGroupIngressArgs(
            description="SSH access to EMR master",
            protocol="tcp",
            from_port=22,
            to_port=22,
            cidr_blocks=["0.0.0.0/0"],
        ),
    ],

    egress=[
        aws.ec2.SecurityGroupEgressArgs(
            description="Allow all outbound traffic",
            protocol="-1",
            from_port=0,
            to_port=0,
            cidr_blocks=["0.0.0.0/0"],
        ),
    ],
)


# ============================================================
# CLUSTER EMR CON HADOOP + HIVE + SPARK
# ============================================================

cluster = aws.emr.Cluster(
    resource_name="retail-tpcds-emr-cluster",

    opts=pulumi.ResourceOptions(delete_before_replace=True),

    name="retail-tpcds-emr-cluster",

    release_label="emr-7.0.0",

    applications=[
        "Hadoop",
        "Hive",
        "Spark",
    ],

    service_role="EMR_DefaultRole",

    ec2_attributes=aws.emr.ClusterEc2AttributesArgs(
        instance_profile="EMR_EC2_DefaultRole",
        key_name=key_name,
        additional_master_security_groups=master_security_group.id,
    ),

    master_instance_group=aws.emr.ClusterMasterInstanceGroupArgs(
        instance_type="m5.xlarge",
        instance_count=1,
    ),

    core_instance_group=aws.emr.ClusterCoreInstanceGroupArgs(
        instance_type="m5.xlarge",
        instance_count=3,
    ),

    log_uri=emr_logs_uri,

    configurations_json="""
    [
      {
        "Classification": "mapred-site",
        "Properties": {
          "mapreduce.framework.name": "yarn"
        }
      },
      {
        "Classification": "hdfs-site",
        "Properties": {
          "dfs.replication": "3"
        }
      },
      {
        "Classification": "spark-defaults",
        "Properties": {
          "spark.sql.sources.partitionOverwriteMode": "dynamic",
          "spark.sql.shuffle.partitions": "12",
          "spark.default.parallelism": "12"
        }
      },
      {
        "Classification": "hive-site",
        "Properties": {
          "hive.execution.engine": "mr"
        }
      }
    ]
    """,

    visible_to_all_users=True,
    termination_protection=False,
    keep_job_flow_alive_when_no_steps=True,
    scale_down_behavior="TERMINATE_AT_TASK_COMPLETION",
)


# ============================================================
# EXPORTS
# ============================================================

pulumi.export("persistent_bucket_name", persistent_bucket_name)

pulumi.export("tpcds_s3_data_path", tpcds_s3_data_path)
pulumi.export("tpcds_s3_warehouse_path", tpcds_s3_warehouse_path)
pulumi.export("emr_logs_path", emr_logs_uri)

pulumi.export("cluster_id", cluster.id)
pulumi.export("master_public_dns", cluster.master_public_dns)

pulumi.export("hdfs_tpcds_raw_path", "/datasets/tpcds/raw")
pulumi.export("hdfs_tpcds_warehouse_path", "/warehouse/tpcds")