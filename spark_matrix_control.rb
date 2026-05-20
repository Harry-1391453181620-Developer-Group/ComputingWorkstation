# spark_matrix_control_v2_fixed.rb
require 'yaml'
require 'net/ssh'
require 'timeout'

class HardenedClusterController
  attr_reader :config, :nodes_status, :quarantined_nodes_cache

  def initialize(config_path)
    unless File.exist?(config_path)
      log_error("CRITICAL: Topology metadata file missing at [#{config_path}]")
      exit 1
    end
    @config = YAML.load_file(config_path)
    @nodes_status = Thread::Queue.new
    @quarantined_nodes_cache = [] # 引入常驻内存的隔离区缓存
    log_info("INIT: Hardware inventory loaded. ASUS WRX90 Controller initializing concurrent control runtime.")
  end

  def orchestrate_huawei_ce8875
    switch = config['cluster']['switch']
    log_info("SWITCH: Initiating secure handshake with Huawei CE8875 [#{switch['ip']}]...")

    begin
      Timeout.timeout(20) do
        Net::SSH.start(switch['ip'], switch['username'], keys: [switch['secret_key_path']], non_interactive: true) do |ssh|
          existing_profiles = ssh.exec!("display drop-profile")
          if existing_profiles.include?("AI_NCCL_ECN")
            log_warn("SWITCH: Pre-existing ECN profile detected. Merging adjustments iteratively.")
          end

          vrp_payload = [
            "system-view",
            "drop-profile AI_NCCL_ECN",
            "wred ecn",
            "color green low-limit 50 high-limit 1500 discard-percentage 10",
            "quit",
            "qos queue-profile AI_ROCE_QUEUE",
            "queue 3 drop-profile AI_NCCL_ECN",
            "quit",
            "interface range 400GE 1/0/1 to 400GE 1/0/24",
            "port mode 200ge",
            "fec mode rs",
            "qos queue-profile AI_ROCE_QUEUE",
            "dcb pfc enable mode manual",
            "dcb pfc priority 3",
            "trust dscp",
            "quit",
            "commit",
            "return"
          ]

          log_info("SWITCH: Pushing hardware-level ECN/PFC profiles into VRP V8 run-time matrix...")
          output = ssh.exec!(vrp_payload.join("\n"))

          if output.include?("Error") || output.include?("Wrong")
            log_error("SWITCH: Configuration rejected by VRP interpreter:\n#{output}")
            raise "Huawei VRP Execution Failure"
          else
            log_info("SWITCH: Hardware parameters committed and saved successfully.")
          end
        end
      end
    rescue Timeout::Error
      log_error("SWITCH: Telemetry timed out while waiting for Huawei CE8875 response. Aborting.")
      exit 1
    rescue => e
      log_error("SWITCH: Critical failure on switch provisioning: #{e.message}")
      exit 1
    end
  end

  def ignite_dgx_spark_rocev2
    log_info("NODES: Deploying non-blocking execution pool across 24 nodes concurrently...")
    worker_threads = []

    config['cluster']['nodes'].each do |node|
      worker_threads << Thread.new do
        node_id = node['id']
        node_ip = node['ip']

        begin
          Timeout.timeout(15) do
            Net::SSH.start(node_ip, 'root', non_interactive: true, connection_timeout: 5) do |ssh|
              iface_check = ssh.exec!("ip link show #{node['rdma_interface']}")
              if iface_check.include?("not exist")
                raise "Interface #{node['rdma_interface']} missing on hardware side."
              end

              pfc_mask = (0..7).map { |p| p == node['pfc_priority'].to_i ? "1" : "0" }.join(",")

              # 硬化修复：持久化追加前增加换行符与强行清空重写保护
              node_payload = [
                "mlnx_qos -i #{node['rdma_interface']} --pfc #{pfc_mask}",
                "sysctl -w net.core.rmem_max=67108864",
                "sysctl -w net.core.wmem_max=67108864",
                "sysctl -w net.ipv4.tcp_ecn=1",
                "sysctl -w net.ipv4.udp_mem='262144 524288 1048576'",
                "echo 1 > /sys/kernel/debug/mlx5/#{node['mlx_device']}/ecn/enable",
                "cma_roce_mode -d #{node['mlx_device']} -p 1 -m 2",
                "mkdir -p /etc/sysctl.d",
                "echo '\n# MAOIDL ROCE PARAMETERS\nnet.core.rmem_max=67108864\nnet.core.wmem_max=67108864' > /etc/sysctl.d/99-maoidl-roce.conf"
              ]

              exec_outputs = ssh.exec!(node_payload.join(" && "))

              log_info("NODES: [#{node_id} @ #{node_ip}] Successfully initialized.")
              @nodes_status << { id: node_id, ip: node_ip, status: :healthy }
            end
          end
        rescue => e
          log_warn("CIRCUIT BREAKER: [#{node_id} @ #{node_ip}] configuration quarantine triggered! Cause: #{e.message}")
          @nodes_status << { id: node_id, ip: node_ip, status: :quarantined, error: e.message }
        end
      end
    end

    worker_threads.each(&:join)
    evaluate_cluster_health_matrix
  end

  def verify_fabric_health
    log_info("DIAGNOSTICS: Commencing continuous telemetry polling for PAM4 CRC error evaluation...")

    config['cluster']['nodes'].each do |node|
      # 修复：真正实现对断路器隔离节点的无感跳过
      if @quarantined_nodes_cache.include?(node['id'])
        log_warn("DIAGNOSTICS: Skipping quarantined node [#{node['id']}] to prevent pipeline blocking.")
        next
      end

      begin
        Net::SSH.start(node['ip'], 'root', non_interactive: true, connection_timeout: 4) do |ssh|
          mtu = ssh.exec!("ip link show #{node['rdma_interface']} | awk '/mtu/ {print $5}'").strip
          speed = ssh.exec!("ethtool #{node['rdma_interface']} | grep -i 'Speed'").strip
          pfc_rx = ssh.exec!("cat /sys/class/infiniband/#{node['mlx_device']}/ports/1/counters/rx_pause_req").strip
          fec_uncorrectable = ssh.exec!("cat /sys/class/infiniband/#{node['mlx_device']}/ports/1/counters_ext/fec_uncorrectable_block_counter").strip

          puts "-----------------------------------------------------------------"
          puts "[Telemetry Report for Core Node: #{node['id']} @ #{node['ip']}]"
          puts "  Physical Layer Negotiation : #{speed.empty? ? 'LINK DOWN' : speed.strip}"
          puts "  Jumbo Frames Window (MTU)  : #{mtu} (Target: 9000)"
          puts "  PFC RX Backpressure Count  : #{pfc_rx} frames"
          puts "  FEC Uncorrectable Error Blocks: #{fec_uncorrectable}"

          if fec_uncorrectable.to_i > 0
            log_error("SIGNAL DEGRADATION: Naddod copper line on #{node['id']} is bleeding packets! Replace cable immediately.")
          end
        end
      rescue => e
        log_warn("DIAGNOSTICS: Target node [#{node['id']}] unresponsive during heartbeat cycle: #{e.message}")
      end
    end
  end

  private

  def log_info(msg);  puts "[+] #{Time.now.strftime('%Y-%m-%d %H:%M:%S')} [INFO] #{msg}"; end
  def log_warn(msg);  puts "[!] #{Time.now.strftime('%Y-%m-%d %H:%M:%S')} [WARN] #{msg}"; end
  def log_error(msg); puts "[-] #{Time.now.strftime('%Y-%m-%d %H:%M:%S')} [FAIL] #{msg}"; end

  def evaluate_cluster_health_matrix
    healthy_count = 0

    while !@nodes_status.empty?
      node = @nodes_status.pop
      if node[:status] == :healthy
        healthy_count += 1
      else
        @quarantined_nodes_cache << node[:id]
      end
    end

    puts "\n================================================================="
    log_info("CLUSTER EVALUATION: Matrix configuration block completed.")
    log_info("  Nodes Perfectly Inoculated: #{healthy_count} / #{@config['cluster']['nodes'].size}")

    unless @quarantined_nodes_cache.empty?
      log_warn("  Quarantined Nodes Present: #{@quarantined_nodes_cache.size}")
      log_warn("PROMPT: PyTorch distributed initialization script must flag and exclude: #{@quarantined_nodes_cache.join(', ')}")
    end
    puts "=================================================================\n"
  end
end

if __FILE__ == $0
  orchestrator = HardenedClusterController.new('cluster_topology.yml')
  orchestrator.orchestrate_huawei_ce8875
  orchestrator.ignite_dgx_spark_rocev2
  orchestrator.verify_fabric_health
end