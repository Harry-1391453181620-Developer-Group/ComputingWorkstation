This unified, production-ready Cluster Orchestration Runbook provides a comprehensive execution manual for your high-performance AI computing setup. To eliminate any context fragmentation or abstraction leakage, the deployment script has been completely optimized. The core orchestration architecture now uses native Ruby multi-threading threads, allowing pixel-perfect injection of node-specific properties (rdma_interface and mlx_device) directly extracted from your topology map. This approach maximizes the processing capabilities of your ASUS WRX90E controller while avoiding the uniform string constraints of standard multi-channel SSH abstractions.

I. Software Precondition
See detail in software.txt

II. Hardware Topology Blueprint (cluster_topology.yml)
Save this file on your ASUS Pro WS WRX90E-SAGE SE master controller.

III. Fabric Master Controller (spark_matrix_control.rb)
Save this file on your master controller. Ensure that your SSH keys for both the Huawei Switch and the DGX Spark instances are pre-seeded.

IV. Execution Manual & Verification Routine
Follow these execution steps on your ASUS WRX90E controller workspace terminal:

1. Environment Preparation
Ensure that Ruby and its core networking components are present on the host OS:

sudo apt-get update
sudo apt-get install -y ruby ruby-dev
sudo gem install net-ssh

2. Runtime Execution
Run the orchestrator with root privileges to allow correct SSH handshake processing:
sudo ruby spark_matrix_control.rb

3. Verification Checklist
To ensure your cluster is fully optimized for AI computing workloads, confirm the following telemetry targets:
| Target Component | Parameter | Verified State | Industrial Purpose |
|---|---|---|---|
| Huawei CE8875 Switch | fec mode rs | Active (Enabled) | Eliminates PAM4 signal degradation on 200G DACs. |
| Huawei CE8875 Switch | wred ecn | Queue 3 Bound | Enables early congestion notification to prevent packet loss. |
| DGX Spark Nodes | MTU | 9000 | Minimizes CPU overhead for large tensor operations. |
| DGX Spark Nodes | cma_roce_mode | 2 (RoCEv2) | Forces hardware-level RDMA transport for data parallel tasks. |
| Linux Kernel Stack | rmem_max / wmem_max | 67108864 (64MB) | Prevents buffer overflows during high-throughput workloads. |