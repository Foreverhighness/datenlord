### 2025-02-08
- introduce async-rdma into datenlord.

```bash
>>> ln -s datenlord/rust-toolchain.toml async-rdma/rust-toolchain.toml
>>> cargo update home@0.5.11 --precise 0.5.9 # follow MSRV
# update tokio version in async-rdma (=1.29.1 -> 1.32.0)
```

### 2025-02-10
- setup build environment.
- fix async-rdma compile error.

### 2025-02-11
- setup testing environment.

```bash
>>> sudo rdma link add rxe_0 type rxe netdev ens3
>>> sudo rdma link show
>>> ibv_devices
```
