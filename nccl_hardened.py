# nccl_hardened.py
import os
import datetime
import torch
import torch.distributed as dist

def init_maoidl_nccl_hardened(
    # Expose BOTH Mellanox HCAs
    os.environ["NCCL_IB_HCA"] = "mlx5_0,mlx5_1"

    # Allow NCCL multi-NIC routing
    os.environ["NCCL_CROSS_NIC"] = "1"

    # RoCEv2
    os.environ["NCCL_IB_GID_INDEX"] = "3"

    # DSCP CS5
    os.environ["NCCL_IB_TC"] = "106"

    # GPUDirect RDMA
    os.environ["NCCL_NET_GDR_LEVEL"] = "5"

    # Multiple queue pairs improve 400G utilization
    os.environ["NCCL_IB_QPS_PER_CONNECTION"] = "4"

    # Split traffic across QPs
    os.environ["NCCL_IB_SPLIT_DATA_ON_QPS"] = "1"

    # More NCCL channels for multi-rail
    os.environ["NCCL_MIN_NCHANNELS"] = "8"

    # PCIe/NUMA-aware P2P
    os.environ["NCCL_P2P_LEVEL"] = "SYS"

    # Larger buffers for VLM gradient spikes
    os.environ["NCCL_BUFFSIZE"] = "8388608"

    # Better reliability
    os.environ["NCCL_ASYNC_ERROR_HANDLING"] = "1"

    # Debugging during stabilization phase
    os.environ["NCCL_DEBUG"] = "WARN"

    # Explicit network interfaces
    os.environ["NCCL_SOCKET_IFNAME"] = "ens6f0np0,ens7f0np0"

    # Avoid IB fallback confusion
    os.environ["NCCL_IB_DISABLE"] = "0"

    os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"

    os.environ["TORCH_NCCL_BLOCKING_WAIT"] = "1"
    os.environ["NCCL_ASYNC_ERROR_HANDLING"] = "1"

    # 4. 异步报错捕获：使用原生的 datetime.timedelta 彻底修复崩溃风险
    dist.init_process_group(
        backend="nccl",
        init_method="env://",
        timeout=datetime.timedelta(seconds=10) # 缩短超时时间，配合上层异构算法做容错重试
    )
    print(f"[+] NCCL Hardened Context fully initialized on Rank {os.environ.get('RANK', '0')}.")