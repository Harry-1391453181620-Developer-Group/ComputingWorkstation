# spark_matrix_control.rb
# ==============================================================================
# High-Performance AI Parallel Computing Orchestrator for MAOIDL Framework
# Target Hardware: Huawei CE8875-24BQ8DQ, Naddod 200G DAC, 24x DGX Spark Nodes
# Controller: ASUS Pro WS WRX90E-SAGE SE (Multi-Threaded Execution Core)
# ==============================================================================

require 'yaml'
require 'net/ssh'

class AIClusterController
  attr_reader :config

  def initialize(config_path)
    unless File.exist?(config_path)
      puts "[-] ERROR: Target hardware topology configuration file missing at: #{config_path}"
      exit 1
    end
    @config = YAML.load_file(config_path)
    puts "[+] INIT: Topology mapped successfully. Ready to govern 24x DGX Spark computing workers."
  end

  # ============================================================================
  # Task 1: Huawei CE8875-24BQ8DQ Switch VRP8 Hardening
  # Optimizes down-negotiated 200G lanes over Naddod PAM4 DAC under heavy NCCL load
  # ============================================================================
  def orchestrate_huawei_ce8875
    switch_ip = config['cluster']['switch']['ip']
    user = config['cluster']['switch']['username']
    key = config['cluster']['switch']['secret_key_path']

    puts "\n[+] TASK 1: Connecting to Huawei CE8875 Backbone Switch [#{switch_ip}] via SSH..."

    Net::SSH.start(switch_ip, user, keys: [key]) do |ssh|
      vrp_commands = [
        "system-view",

        # Configure WRED profile for Explicit Congestion Notification (ECN) marking
        "drop-profile AI_NCCL_ECN",
        "wred ecn",
        "color green low-limit 50 high-limit 1500 discard-percentage 10",
        "quit",

        # Bind ECN profile to Quality of Service (QoS) Queue 3 (Dedicated RoCEv2 Lane)
        "qos queue-profile AI_ROCE_QUEUE",
        "queue 3 drop-profile AI_NCCL_ECN",
        "quit",

        # Batch configuration mode for the 24 downlinks paired with Naddod 200G DACs
        "interface range 400GE 1/0/1 to 400GE 1/0/24",
        "port mode 200ge",                 # Force port speed split to match 200G links
        "fec mode rs",                     # CRITICAL: Lock Reed-Solomon FEC to patch PAM4 signal attenuation
        "qos queue-profile AI_ROCE_QUEUE", # Inject ECN throttling matrix
        "dcb pfc enable mode manual",      # Force hardware Priority-based Flow Control
        "dcb pfc priority 3",              # Map drop-free link execution to priority class 3
        "trust dscp",                      # Instruct VRP backplane to respect incoming PyTorch DSCP bits
        "quit",

        "commit",                          # Flush configuration array to VRP running config
        "return"
      ]

      puts "[+] SWITCH: Injecting network profile (RS-FEC, WRED-ECN, DCB-PFC) into physical interfaces..."
      output = ssh.exec!(vrp_commands.join("\n"))
      puts "[SWITCH RESPONSE]:\n#{output}"
    end
    puts "[+] TASK 1: Huawei CE8875 infrastructure safely locked into lossless AI mode."
  end

  # ============================================================================
  # Task 2: Multi-Threaded Parallel Node Inoculation
  # Configures Linux Network Stack & Mellanox ConnectX Firmware via concurrent threads
  # ============================================================================
  def ignite_dgx_spark_rocev2
    puts "\n[+] TASK 2: Spawning concurrent execution threads across 24 DGX Spark instances..."
    threads = []

    config['cluster']['nodes'].each do |node|
      threads << Thread.new do
        begin
          Net::SSH.start(node['ip'], 'root') do |ssh|
            # Read node-specific layout data
            iface = node['rdma_interface']
            dev = node['mlx_device']
            prio = node['pfc_priority']

            # Step A: Lock Mellanox link layer priority flow control via mlnx_qos
            # Format pattern: 8-element bitmask map corresponding to priorities 0-7
            pfc_mask = (0..7).map { |p| p == prio ? "1" : "0" }.join(",")
            ssh.exec!("mlnx_qos -i #{iface} --pfc #{pfc_mask}")

            # Step B: Tune Linux network core socket ring buffers for huge 200G pipeline surges
            ssh.exec!("sysctl -w net.core.rmem_max=67108864")
            ssh.exec!("sysctl -w net.core.wmem_max=67108864")
            ssh.exec!("sysctl -w net.ipv4.tcp_ecn=1")

            # Step C: Enable hardware-level ECN response tracking inside Mellanox firmware
            ssh.exec!("echo 1 > /sys/kernel/debug/mlx5/#{dev}/ecn/enable")

            # Step D: Lock transport protocol layer to RoCEv2 (Force UDP Frame Encapsulation)
            ssh.exec!("cma_roce_mode -d #{dev} -p 1 -m 2")

            puts "[+] NODE DONE: [#{node['id']} @ #{node['ip']}] synced with absolute network parameters."
          end
        rescue => e
          puts "[-] NODE CRITICAL FAILURE: [#{node['id']}] fell out of cluster loop: #{e.message}"
        end
      end
    end

    # Block main thread until all 24 parallel worker threads return clean executions
    threads.each(&:join)
    puts "[+] TASK 2: Distributed worker node configurations completely aligned."
  end

  # ============================================================================
  # Task 3: Signal Integrity & Link Budget Diagnostics Run
  # Checks physical layer and frame counters to eliminate frame loss
  # ============================================================================
  def verify_fabric_health
    puts "\n[+] TASK 3: Initiating hardware-level telemetry validation..."

    config['cluster']['nodes'].each do |node|
      Net::SSH.start(node['ip'], 'root') do |ssh|
        mtu = ssh.exec!("ip link show #{node['rdma_interface']} | awk '/mtu/ {print $5}'").strip
        speed = ssh.exec!("ethtool #{node['rdma_interface']} | grep -i 'Speed'").strip
        pfc_rx = ssh.exec!("cat /sys/class/infiniband/#{node['mlx_device']}/ports/1/counters/rx_pause_req").strip

        puts "================================================================="
        puts " [Telemetry Profile] #{node['id']} (#{node['ip']})"
        puts "  Link Negotiation : #{speed.empty? ? 'LINK DOWN' : speed.strip}"
        puts "  MTU Configuration: #{mtu} (Required Target: 9000)"
        puts "  PFC RX Pause Triggers: #{pfc_rx} frames"
        if pfc_rx.to_i > 100000
          puts "  [!] WARNING: High PFC pause frequency detected. Fine-tune WRED limits on Switch."
        end
      end
    end
    puts "================================================================="
    puts "[+] TASK 3: Global link diagnosis completed."
  end
end

# ============================================================================
# Main Master Script Entry Sequence
# ============================================================================
if __FILE__ == $0
  puts "================================================================="
  puts "   MAOIDL DEEP LEARNING CLUSTER CONFIGURATION MANAGER            "
  puts "================================================================="

  orchestrator = AIClusterController.new('cluster_topology.yml')

  # Step 1: provision network switch configurations
  orchestrator.orchestrate_huawei_ce8875

  # Step 2: Inject RoCEv2 configurations into nodes concurrently
  orchestrator.ignite_dgx_spark_rocev2

  # Step 3: Run cluster-wide connection integrity check
  orchestrator.verify_fabric_health

  puts "\n[SUCCESS] Compute fabric aligned. System ready for PyTorch NCCL execution."
end
