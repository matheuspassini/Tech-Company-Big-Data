# YARN Directory

This directory contains Hadoop/YARN configuration files for the data lake cluster.

## Purpose

YARN (Yet Another Resource Negotiator) is Hadoop's resource management and job scheduling system that:
- Manages cluster resources
- Schedules Spark applications
- Provides resource isolation
- Enables multi-tenant cluster usage

## Configuration Files

### Core Configuration
- **yarn-site.xml**: YARN daemon configurations
- **core-site.xml**: Hadoop core configurations
- **hdfs-site.xml**: HDFS configurations

### Key Settings

#### Resource Management
```xml
<property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>8192</value>
</property>
```

#### Application Master
```xml
<property>
    <name>yarn.app.mapreduce.am.resource.mb</name>
    <value>2048</value>
</property>
```

#### Container Memory
```xml
<property>
    <name>yarn.scheduler.minimum-allocation-mb</name>
    <value>512</value>
</property>
```

## Cluster Architecture

### Master Node
- **ResourceManager**: Manages cluster resources
- **NameNode**: Manages HDFS metadata
- **History Server**: Job history tracking

### Worker Nodes
- **NodeManager**: Manages node resources
- **DataNode**: Stores HDFS data blocks

## Web Interfaces

### YARN Web UI
- **URL**: http://localhost:8081
- **Purpose**: Monitor applications and cluster resources
- **Features**: Application tracking, resource usage, job history

### HDFS Web UI
- **URL**: http://localhost:9870
- **Purpose**: Browse HDFS file system
- **Features**: File browser, cluster status, data node info

## Monitoring

### Key Metrics
- **Cluster Memory**: Total available memory
- **Application Count**: Running applications
- **Container Usage**: Resource utilization
- **Queue Status**: Job queue information

### Logs
- **ResourceManager Logs**: Cluster management logs
- **NodeManager Logs**: Node-specific logs
- **Application Logs**: Individual job logs

## Troubleshooting

### Common Issues
1. **Resource Exhaustion**: Increase memory allocation
2. **Application Failures**: Check application logs
3. **Node Failures**: Verify node connectivity
4. **Queue Congestion**: Adjust queue configurations

### Debug Commands
```bash
# Check YARN status
yarn application -list

# View application logs
yarn logs -applicationId <app_id>

# Check cluster resources
yarn node -list
``` 