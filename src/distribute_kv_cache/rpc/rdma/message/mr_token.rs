use std::mem;
use std::time::Duration;
use std::time::SystemTime;

use async_rdma::MrToken;
use bytes::Buf;
use bytes::BufMut;
use bytes::BytesMut;
use clippy_utilities::OverflowArithmetic;

use crate::async_fuse::util::usize_to_u64;
use crate::distribute_kv_cache::rpc::error::RpcError;
use crate::distribute_kv_cache::rpc::packet::{ActualSize, Decode, Encode};
use crate::distribute_kv_cache::rpc::utils::u64_to_usize;

impl Encode for MrToken {
    fn encode(&self, buf: &mut BytesMut) {
        let addr = usize_to_u64(self.addr);
        let len = usize_to_u64(self.len);

        buf.put_u64_le(addr);
        buf.put_u64_le(len);
        buf.put_u32_le(self.rkey);
        // buf.put_slice(self.ddl.to_bytes()); // deadline feature is not used in async_rdma
        buf.put_u32_le(self.access);
    }
}

impl Decode for MrToken {
    fn decode(buf: &mut BytesMut) -> Result<Self, RpcError>
    where
        Self: Sized,
    {
        const DEFAULT_RMR_TIMEOUT: Duration = Duration::from_secs(60);

        if buf.len() < 24 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }

        let addr = u64_to_usize(buf.get_u64_le());
        let len = u64_to_usize(buf.get_u64_le());
        let rkey = buf.get_u32_le();
        let access = buf.get_u32_le();

        let mr_token = MrToken {
            addr,
            len,
            rkey,
            ddl: SystemTime::now().checked_add(DEFAULT_RMR_TIMEOUT).unwrap(),
            access,
        };
        Ok(mr_token)
    }
}

impl ActualSize for MrToken {
    fn actual_size(&self) -> u64 {
        let addr_len = usize_to_u64(mem::size_of_val(&self.addr));
        let len_len = usize_to_u64(mem::size_of_val(&self.len));
        let rkey_len = usize_to_u64(mem::size_of_val(&self.rkey));
        let access_len = usize_to_u64(mem::size_of_val(&self.access));
        addr_len
            .overflow_add(len_len)
            .overflow_add(rkey_len)
            .overflow_add(access_len)
    }
}
