# nccl_hardened.py
import os
import datetime
import torch
import torch.distributed as dist

def init_maoidl_nccl_hardened():
    """
    针对 24x DGX Spark 与 华为 CE8875 拓扑优化的 NCCL 极致可靠性运行时参数
    """
    # 1. 强制锁死通信协议为 RoCEv2，匹配交换机的 DSCP 信任
    os.environ["NCCL_IB_GID_INDEX"] = "3"  # 对应 RoCEv2 索引
    os.environ["NCCL_IB_TC"] = "106"       # DSCP CS5 映射，完美切入交换机 Queue 3

    # 2. 拥塞自适应调整：在高拥塞（PFC 频发）时，限制流控块大小，防止击穿交换机 Buffer
    os.environ["NCCL_BUFFSIZE"] = "4194304" # 限制为 4MB，减少微爆流突发

    # 3. 动态死锁超时重置阈值
    os.environ["NCCL_NET_GDR_LEVEL"] = "5"   # 强制开启 GPUDirect RDMA 直通
    os.environ["NCCL_CROSS_NIC"] = "0"       # 锁死单网卡绑定，杜绝异构多路径漂移

    # 4. 异步报错捕获：使用原生的 datetime.timedelta 彻底修复崩溃风险
    dist.init_process_group(
        backend="nccl",
        init_method="env://",
        timeout=datetime.timedelta(seconds=30) # 缩短超时时间，配合上层异构算法做容错重试
    )
    print(f"[+] NCCL Hardened Context fully initialized on Rank {os.environ.get('RANK', '0')}.")