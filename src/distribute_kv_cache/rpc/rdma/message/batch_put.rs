use core::mem;

use bytes::{Buf, BufMut, BytesMut};
use clippy_utilities::OverflowArithmetic;

use crate::async_fuse::util::usize_to_u64;
use crate::distribute_kv_cache::rpc::error::RpcError;
use crate::distribute_kv_cache::rpc::message::KVBlockBatchPutResponse;
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
    fn decode(buf: &mut BytesMut) -> Result<Self, RpcError> {
        if buf.len() < 8 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let batch_size = u64_to_usize(buf.get_u64_le());
        let mut put_requests = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            put_requests.push(KVBlockPutRequestWithRdma::decode(buf)?);
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
    /// The response to put kv blocks.
    pub batch_put_response: KVBlockBatchPutResponse,
}

impl Encode for KVBlockBatchPutResponseWithRdma {
    /// Encode the kv block batch put response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        self.batch_put_response.encode(buf);
    }
}

impl ActualSize for KVBlockBatchPutResponseWithRdma {
    fn actual_size(&self) -> u64 {
        unimplemented!("underlay KVBlockBatchPutResponse has not impl this method")
    }
}

impl Decode for KVBlockBatchPutResponseWithRdma {
    /// Decode the byte buffer into a kv block get response.
    fn decode(buf: &mut BytesMut) -> Result<Self, RpcError> {
        let batch_put_response = KVBlockBatchPutResponse::decode(buf).unwrap();
        Ok(Self { batch_put_response })
    }
}
