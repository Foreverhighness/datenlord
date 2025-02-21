use core::mem;
use std::sync::Arc;

use async_rdma::{LocalMr, MrToken};
use bytes::{Buf, BufMut, BytesMut};
use clippy_utilities::OverflowArithmetic;

use crate::async_fuse::util::usize_to_u64;
use crate::distribute_kv_cache::rpc::error::RpcError;
use crate::distribute_kv_cache::rpc::packet::{ActualSize, Decode, Encode};

/// The request to put a single kv block.
#[derive(Debug, Clone)]
pub struct KVBlockPutRequestWithRdma {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
    /// The mr token.
    pub mr_token: MrToken,
    /// The local mr, use to extend lifetime.
    pub _local_mr: Option<Arc<LocalMr>>,
}

impl Encode for KVBlockPutRequestWithRdma {
    /// Encode the kv block put request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.block_size);
        buf.put_u64_le(self.kv_cache_id);
        self.mr_token.encode(buf);
    }
}

impl Decode for KVBlockPutRequestWithRdma {
    /// Decode the byte buffer into a kv block put request.
    fn decode_u8_buf(mut buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 40 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = buf.get_u64_le();
        let kv_cache_id = buf.get_u64_le();
        let mr_token = MrToken::decode_u8_buf(buf).unwrap();
        Ok(Self {
            block_size,
            kv_cache_id,
            mr_token,
            _local_mr: None,
        })
    }
}

impl ActualSize for KVBlockPutRequestWithRdma {
    fn actual_size(&self) -> u64 {
        let block_size_len = usize_to_u64(mem::size_of_val(&self.block_size));
        let kv_cache_id_len = usize_to_u64(mem::size_of_val(&self.kv_cache_id));
        let mr_token_size = self.mr_token.actual_size();
        block_size_len
            .overflow_add(kv_cache_id_len)
            .overflow_add(mr_token_size)
    }
}
