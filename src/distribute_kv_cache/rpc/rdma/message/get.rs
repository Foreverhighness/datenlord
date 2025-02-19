use core::mem;

use async_rdma::MrToken;
use bytes::{Buf, BufMut, BytesMut};
use clippy_utilities::OverflowArithmetic;

use crate::async_fuse::util::usize_to_u64;
use crate::distribute_kv_cache::rpc::error::RpcError;
use crate::distribute_kv_cache::rpc::message::StatusCode;
use crate::distribute_kv_cache::rpc::packet::{ActualSize, Decode, Encode};

/// The request to get kv block, by using RDMA.
#[derive(Debug)]
pub struct KVBlockGetRequestWithRdma {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
    /// The mr token.
    pub mr_token: MrToken,
}

impl Encode for KVBlockGetRequestWithRdma {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.block_size);
        buf.put_u64_le(self.kv_cache_id);
        self.mr_token.encode(buf);
    }
}

impl Decode for KVBlockGetRequestWithRdma {
    /// Decode the byte buffer into a kv block get request.
    fn decode(buf: &mut BytesMut) -> Result<Self, RpcError> {
        if buf.len() < 40 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = buf.get_u64_le();
        let kv_cache_id = buf.get_u64_le();
        let mr_token = MrToken::decode(buf).unwrap();
        Ok(Self {
            block_size,
            kv_cache_id,
            mr_token,
        })
    }
}

impl ActualSize for KVBlockGetRequestWithRdma {
    fn actual_size(&self) -> u64 {
        let block_size_len = usize_to_u64(mem::size_of_val(&self.block_size));
        let kv_cache_id_len = usize_to_u64(mem::size_of_val(&self.kv_cache_id));
        block_size_len
            .overflow_add(kv_cache_id_len)
            .overflow_add(self.mr_token.actual_size())
    }
}

/// The response to get kv block.
#[derive(Debug)]
pub struct KVBlockGetResponseWithRdma {
    /// The kv block size.
    pub block_size: u64,
    /// The kv cache id.
    pub kv_cache_id: u64,
    /// The status of the response.
    pub status: StatusCode,
}

impl Encode for KVBlockGetResponseWithRdma {
    /// Encode the kv block get response into a byte buffer.
    fn encode(&self, buf: &mut BytesMut) {
        // FIXME: check `capacity-len` or remove this check
        if buf.capacity() < 17 {
            buf.reserve(17);
        }
        buf.put_u64_le(self.block_size);
        buf.put_u64_le(self.kv_cache_id);
        match self.status {
            StatusCode::Success => buf.put_u8(0),
            StatusCode::NotFound => buf.put_u8(1),
            StatusCode::InternalError => buf.put_u8(2),
            // Not used here.
            StatusCode::VersionMismatch => buf.put_u8(3),
        }
    }
}

impl Decode for KVBlockGetResponseWithRdma {
    fn decode(buf: &mut BytesMut) -> Result<Self, RpcError>
    where
        Self: Sized,
    {
        if buf.len() < 17 {
            return Err(RpcError::InternalError("Insufficient bytes".to_owned()));
        }
        let block_size = buf.get_u64_le();
        let kv_cache_id = buf.get_u64_le();
        let status = match buf.get_u8() {
            0 => StatusCode::Success,
            1 => StatusCode::NotFound,
            2 => StatusCode::InternalError,
            3 => StatusCode::VersionMismatch,
            _ => return Err(RpcError::InternalError("Invalid status code".to_owned())),
        };

        Ok(KVBlockGetResponseWithRdma {
            block_size,
            kv_cache_id,
            status,
        })
    }
}
