# SSH Configuration

This directory contains SSH configuration files for secure communication within the Spark/Hadoop cluster.

## Structure

```
ssh/
├── ssh_config    # SSH client configuration
└── README.md     # This file
```

## SSH Configuration Overview

### `ssh_config`
SSH client configuration file that defines connection parameters for secure communication between cluster nodes.

**Configuration Details:**
```bash
Host *
  UserKnownHostsFile /dev/null
  StrictHostKeyChecking no
```

## Configuration Parameters

### `Host *`
- **Purpose**: Applies configuration to all hosts
- **Scope**: Global configuration for all SSH connections
- **Usage**: Ensures consistent behavior across all cluster nodes

### `UserKnownHostsFile /dev/null`
- **Purpose**: Disables host key caching
- **Security Impact**: 
  - **Pros**: Simplifies containerized environments
  - **Cons**: Reduces security verification
- **Usage**: Prevents SSH from storing host keys in known_hosts file

### `StrictHostKeyChecking no`
- **Purpose**: Disables strict host key verification
- **Security Impact**:
  - **Pros**: Allows automatic connections without manual verification
  - **Cons**: Reduces protection against man-in-the-middle attacks
- **Usage**: Enables seamless container-to-container communication

## Security Considerations

### Containerized Environment
In Docker-based Spark/Hadoop clusters:
- **Host Keys**: May change frequently due to container restarts
- **Network Isolation**: Internal cluster communication is isolated
- **Automation**: Requires automatic connection establishment

### Security Trade-offs
| Feature | Security Level | Convenience | Use Case |
|---------|---------------|-------------|----------|
| `UserKnownHostsFile /dev/null` | Lower | Higher | Development/Testing |
| `StrictHostKeyChecking no` | Lower | Higher | Automated deployments |

### Production Recommendations
For production environments, consider:
```bash
# More secure configuration
Host *
  UserKnownHostsFile ~/.ssh/known_hosts
  StrictHostKeyChecking yes
  ServerAliveInterval 60
  ServerAliveCountMax 3
```

## Usage in Project

### Docker Integration
```dockerfile
# Copy SSH configuration to container
COPY ssh/ssh_config /root/.ssh/config
RUN chmod 600 /root/.ssh/config
```

### Cluster Communication
```bash
# SSH between cluster nodes
ssh master
ssh worker1
ssh worker2

# Execute commands remotely
ssh master "hdfs dfs -ls /"
ssh worker1 "yarn node -list"
```

### Service Discovery
```bash
# Check cluster connectivity
for node in master worker1 worker2; do
  ssh $node "hostname"
done
```

## Troubleshooting

### Common SSH Issues

#### Connection Refused
```bash
# Check if SSH service is running
docker exec tech-data-lake-master service ssh status

# Restart SSH service if needed
docker exec tech-data-lake-master service ssh restart
```

#### Permission Denied
```bash
# Check SSH key permissions
ls -la ~/.ssh/

# Fix permissions
chmod 600 ~/.ssh/id_rsa
chmod 644 ~/.ssh/id_rsa.pub
chmod 700 ~/.ssh/
```

#### Host Key Verification Failed
```bash
# Clear known hosts (if using default config)
rm ~/.ssh/known_hosts

# Or use the project's config
cp ssh/ssh_config ~/.ssh/config
```

### Debugging SSH Connections
```bash
# Verbose SSH output
ssh -v master

# Test connection with specific config
ssh -F ssh/ssh_config master
```

## Monitoring and Logging

### SSH Connection Monitoring
```bash
# Monitor SSH connections
docker exec tech-data-lake-master netstat -an | grep :22

# Check SSH logs
docker exec tech-data-lake-master tail -f /var/log/auth.log
```

### Cluster Health Checks
```bash
# Verify all nodes are reachable
for node in master worker1 worker2; do
  echo "Testing $node..."
  ssh $node "echo 'Connection successful'"
done
```

## Best Practices

### Development Environment
- Use the provided `ssh_config` for seamless development
- Accept security trade-offs for convenience
- Focus on functionality over strict security

### Testing and Validation
```bash
# Test SSH configuration
ssh -F ssh/ssh_config master "echo 'SSH config working'"

# Validate cluster connectivity
./test_ssh_connectivity.sh
```

### Documentation
- Document any SSH-related issues and solutions
- Keep configuration changes minimal
- Test SSH connectivity after cluster changes

## Alternative Configurations

### Enhanced Security (Optional)
```bash
# More secure configuration for production
Host *
  UserKnownHostsFile ~/.ssh/known_hosts
  StrictHostKeyChecking yes
  ServerAliveInterval 60
  ServerAliveCountMax 3
  ConnectTimeout 10
  BatchMode yes
```

### Debug Configuration
```bash
# Debug configuration for troubleshooting
Host *
  UserKnownHostsFile /dev/null
  StrictHostKeyChecking no
  LogLevel DEBUG3
  VerboseMode yes
```

## Integration with Spark/Hadoop

### YARN Resource Manager
```bash
# SSH to resource manager
ssh master "yarn application -list"

# Check node status
ssh worker1 "yarn node -status"
```

### HDFS NameNode
```bash
# SSH to name node
ssh master "hdfs dfsadmin -report"

# Check HDFS status
ssh master "hdfs dfsadmin -safemode get"
```

### Spark History Server
```bash
# Access Spark history
ssh master "curl -s http://localhost:18081"
```

## Maintenance

### Regular Checks
- **Weekly**: Verify SSH connectivity between all nodes
- **Monthly**: Review SSH configuration for security updates
- **After Updates**: Test SSH connections after cluster updates

### Backup and Recovery
```bash
# Backup SSH configuration
cp ssh/ssh_config ssh/ssh_config.backup

# Restore SSH configuration
cp ssh/ssh_config.backup ssh/ssh_config
```
