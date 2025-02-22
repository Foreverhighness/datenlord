use core::mem;

use bytes::{Buf, BufMut, BytesMut};
use clippy_utilities::OverflowArithmetic;

use crate::async_fuse::util::usize_to_u64;
use crate::distribute_kv_cache::rpc::error::RpcError;
use crate::distribute_kv_cache::rpc::packet::{ActualSize, Decode, Encode};
use crate::distribute_kv_cache::rpc::utils::u64_to_usize;

use super::KVBlockPutRequestWithRdma;

/// The request to put multiple kv blocks.
#[derive(Debug, Clone)]
pub struct KVBlockBatchPutRequestWithRdma {
    /// A list of mr tokens.
    pub put_requests: Vec<KVBlockPutRequestWithRdma>,
}

impl Encode for KVBlockBatchPutRequestWithRdma {
    /// Encode the kv block batch put request into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        let batch_size = usize_to_u64(self.put_requests.len());
        buf.put_u64_le(batch_size);
        for req in &self.put_requests {
            req.encode(buf);
        }
    }
}

impl Decode for KVBlockBatchPutRequestWithRdma {
    /// Decode the byte buffer into a kv block batch put request.
    fn decode_u8_buf(mut buf: &[u8]) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let batch_size = u64_to_usize(buf.get_u64_le());
        let mut put_requests = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            put_requests.push(KVBlockPutRequestWithRdma::decode_u8_buf(buf)?);
        }
        Ok(Self { put_requests })
    }
}

impl ActualSize for KVBlockBatchPutRequestWithRdma {
    fn actual_size(&self) -> u64 {
        let batch_size = usize_to_u64(self.put_requests.len());
        let mut size = usize_to_u64(mem::size_of_val(&batch_size));
        for req in &self.put_requests {
            size = size.overflow_add(req.actual_size());
        }
        size
    }
}

/// The response to put multiple kv blocks.
#[derive(Debug, Clone)]
pub struct KVBlockBatchPutResponseWithRdma {
    /// The success kv cache ids.
    pub success_kv_cache_ids: Vec<u64>,
    /// The failed kv cache ids.
    pub failed_kv_cache_ids: Vec<u64>,
}

impl Encode for KVBlockBatchPutResponseWithRdma {
    /// Encode the kv block batch put response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        let ids = &self.success_kv_cache_ids;

        let batch_size = usize_to_u64(ids.len());
        buf.put_u64_le(batch_size);
        for &id in ids {
            buf.put_u64_le(id);
        }

        let ids = &self.failed_kv_cache_ids;

        let batch_size = usize_to_u64(ids.len());
        buf.put_u64_le(batch_size);
        for &id in ids {
            buf.put_u64_le(id);
        }
    }
}

impl ActualSize for KVBlockBatchPutResponseWithRdma {
    fn actual_size(&self) -> u64 {
        let num_of_elem = self.success_kv_cache_ids.len() + self.failed_kv_cache_ids.len();
        usize_to_u64(
            mem::size_of::<u64>() + mem::size_of::<u64>() + mem::size_of::<u64>() * num_of_elem,
        )
    }
}

impl Decode for KVBlockBatchPutResponseWithRdma {
    /// Decode the byte buffer into a kv block get response.
    fn decode(buf: &mut BytesMut) -> Result<Self, RpcError> {
        if buf.len() < 16 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }

        let size = u64_to_usize(buf.get_u64_le());
        let mut success_kv_cache_ids = Vec::with_capacity(size);
        for _ in 0..size {
            success_kv_cache_ids.push(buf.get_u64_le());
        }

        let size = u64_to_usize(buf.get_u64_le());
        let mut failed_kv_cache_ids = Vec::with_capacity(size);
        for _ in 0..size {
            failed_kv_cache_ids.push(buf.get_u64_le());
        }

        Ok(Self {
            success_kv_cache_ids,
            failed_kv_cache_ids,
        })
    }
}
