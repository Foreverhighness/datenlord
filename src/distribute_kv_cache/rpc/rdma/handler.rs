use std::alloc::Layout;
use std::sync::Arc;

use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, Rdma, RemoteMr};
use async_trait::async_trait;
use bytes::BytesMut;
use tokio::sync::mpsc;
use tracing::{debug, error};

use crate::distribute_kv_cache::local_cache::block::{Block, MetaData};
use crate::distribute_kv_cache::local_cache::manager::KVBlockManager;
use crate::distribute_kv_cache::rpc::message::{ReqType, RespType, StatusCode};
use crate::distribute_kv_cache::rpc::packet::{
    self, ActualSize, Decode, Encode, ReqHeader, RespHeader,
};
use crate::distribute_kv_cache::rpc::rdma::message::{
    KVBlockBatchPutResponseWithRdma, KVBlockGetRequestWithRdma, KVBlockGetResponseWithRdma,
};
use crate::distribute_kv_cache::rpc::utils::u64_to_usize;
use crate::distribute_kv_cache::rpc::workerpool::Job;

use super::message::KVBlockBatchPutRequestWithRdma;

/// The handler for the RPC kv cache block rdma request.
#[derive(Debug)]
pub struct KVBlockRdmaHandler {
    /// The request header.
    header: ReqHeader,
    /// The request body.
    request: bytes::Bytes,
    /// The channel for sending the response.
    done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
    /// Local index manager
    cache_manager: Arc<KVBlockManager>,
    /// RDMA handler
    rdma: Arc<Rdma>,
}

impl KVBlockRdmaHandler {
    /// Create a new kv block rdma handler.
    #[must_use]
    pub fn new(
        header: ReqHeader,
        request: bytes::Bytes,
        done_tx: mpsc::Sender<Vec<bytes::Bytes>>,
        cache_manager: Arc<KVBlockManager>,
        rdma: Arc<Rdma>,
    ) -> Self {
        Self {
            header,
            request,
            done_tx,
            cache_manager,
            rdma,
        }
    }
}

#[async_trait]
impl Job for KVBlockRdmaHandler {
    /// KV block handler rdma inner run.
    /// Support Block get and batch put.
    async fn run(&self) {
        let req_type = match ReqType::from_u8(self.header.op) {
            Ok(req_type) => req_type,
            Err(err) => {
                error!("Invalid request type: {op}, {err:?}", op = self.header.op);
                return;
            }
        };

        let req_buffer = &self.request[..];
        let mut resp_bytes_vec = vec![];

        println!("KVBlockRdmaHandler run req_type: {req_type:?}");

        match req_type {
            ReqType::KVBlockGetRequestWithRdma => {
                let req = match KVBlockGetRequestWithRdma::decode_u8_buf(req_buffer) {
                    Ok(req) => req,
                    Err(err) => {
                        debug!("Failed to decode file block request: {:?}", err);
                        return;
                    }
                };

                println!("KVBlockRdmaHandler run KVBlockGetRequestWithRdma req: {req:?}");
                let KVBlockGetRequestWithRdma {
                    block_size,
                    kv_cache_id,
                    mr_token,
                } = req;

                // Default response
                let mut response = KVBlockGetResponseWithRdma {
                    kv_cache_id,
                    block_size,
                    status: StatusCode::InternalError,
                };

                // Get the block by id
                let metadata = MetaData::new(kv_cache_id, 0, 0, 0);
                if let Ok(Some(block)) = self.cache_manager.read(metadata).await {
                    let data = block.read().unwrap().get_data();

                    // Put block data to remote mr
                    let layout = Layout::from_size_align(u64_to_usize(block_size), 4096).unwrap();
                    // Safety: immediate initialize
                    let mut local_mr = match unsafe { self.rdma.alloc_local_mr_uninit(layout) } {
                        Ok(local_mr) => local_mr,
                        Err(err) => {
                            error!("Failed to allocate local mr: {err:?}");
                            // TODO(fh): decision on how to handle this error? immediately return or response with failed message?
                            return;
                        }
                    };
                    println!("RDMA alloc local_mr success");

                    debug_assert_eq!(data.len(), u64_to_usize(block_size));

                    local_mr.as_mut_slice().copy_from_slice(&data);
                    println!(
                        "FH: get block data: {data:?}",
                        data = &local_mr.as_slice()[..30]
                    );

                    let mut remote_mr = RemoteMr::new(mr_token);

                    match self.rdma.write(&local_mr, &mut remote_mr).await {
                        Ok(()) => {
                            response = KVBlockGetResponseWithRdma {
                                kv_cache_id,
                                block_size,
                                status: StatusCode::Success,
                            };
                        }
                        Err(err) => {
                            error!("Failed to write remote mr: {err:?}");
                        }
                    }
                }

                let resp_header = RespHeader {
                    seq: self.header.seq,
                    op: RespType::KVBlockGetResponseWithRdma.to_u8(),
                    len: response.actual_size(),
                };
                let mut buffer = BytesMut::with_capacity(u64_to_usize(
                    packet::RESP_HEADER_SIZE + response.actual_size(),
                ));

                resp_header.encode(&mut buffer);
                response.encode(&mut buffer);

                resp_bytes_vec.push(buffer.freeze());
            }
            ReqType::KVBlockBatchPutRequestWithRdma => {
                let req = match KVBlockBatchPutRequestWithRdma::decode_u8_buf(req_buffer) {
                    Ok(req) => req,
                    Err(err) => {
                        debug!("Failed to decode file block request: {:?}", err);
                        return;
                    }
                };

                debug_assert!(
                    !req.put_requests.is_empty(),
                    "receive batch put request without elements."
                );

                println!("KVBlockRdmaHandler run KVBlockBatchPutRequestWithRdma req: {req:?}");

                let mut success_kv_cache_ids = vec![];
                let mut failed_kv_cache_ids = vec![];

                // FIXME(fh): Only allow one size for kv blocks?
                let block_size = req.put_requests[0].block_size;
                debug_assert!(req
                    .put_requests
                    .iter()
                    .all(|req| req.block_size == block_size));
                let block_size = u64_to_usize(block_size);

                let layout = std::alloc::Layout::from_size_align(block_size, 4096).unwrap();

                // Safety: immediate initialize
                let mut local_mr = match unsafe { self.rdma.alloc_local_mr_uninit(layout) } {
                    Ok(local_mr) => local_mr,
                    Err(err) => {
                        error!("Failed to allocate local mr: {err:?}");
                        // TODO(fh): decision on how to handle this error? immediately return or response with all failed ids?
                        return;
                    }
                };
                println!("RDMA alloc local_mr success");

                for req in req.put_requests {
                    let mr_token = req.mr_token;
                    let cache_id = req.kv_cache_id;

                    let remote_mr = RemoteMr::new(mr_token);

                    if let Err(err) = self.rdma.read(&mut local_mr, &remote_mr).await {
                        error!("Failed to read remote mr: {err:?}");
                        failed_kv_cache_ids.push(cache_id);
                        continue;
                    }

                    let data = local_mr.as_slice();
                    println!("FH: put block data: {data:?}", data = &data[..30]);

                    let block_start = tokio::time::Instant::now();
                    let meta_data = MetaData::new(cache_id, 0, 0, 0);
                    let kv_block = Block::new(meta_data, bytes::Bytes::from(data.to_vec()));
                    let block_start_0 = block_start.elapsed();
                    debug!(
                        "KVBlockBatchPutRequest new block: Time elapsed: {:?}",
                        block_start_0
                    );
                    match self.cache_manager.write(kv_block).await {
                        Ok(()) => {
                            success_kv_cache_ids.push(cache_id);
                            let block_start_1 = block_start.elapsed();
                            debug!(
                                "KVBlockBatchPutRequest write block: Time elapsed: {:?}",
                                block_start_1 - block_start_0
                            );
                        }
                        Err(err) => {
                            error!("Failed to put block into cache: {:?}", err);
                            failed_kv_cache_ids.push(cache_id);
                        }
                    }
                }

                // Prepare response
                let batch_put_resp = KVBlockBatchPutResponseWithRdma {
                    success_kv_cache_ids,
                    failed_kv_cache_ids,
                };
                let resp_header = RespHeader {
                    seq: self.header.seq,
                    op: RespType::KVBlockBatchPutResponseWithRdma.to_u8(),
                    len: batch_put_resp.actual_size(),
                };
                let mut buffer = BytesMut::with_capacity(u64_to_usize(
                    packet::RESP_HEADER_SIZE + batch_put_resp.actual_size(),
                ));

                resp_header.encode(&mut buffer);
                batch_put_resp.encode(&mut buffer);

                resp_bytes_vec.push(buffer.freeze());
            }
            _ => {
                error!("Invalid request type: {req_type:?}");
            }
        }

        match self.done_tx.send(resp_bytes_vec).await {
            Ok(()) => {
                debug!("Send {req_type:?} response to done channel");
            }
            Err(err) => {
                error!("Failed to send response tod done channel: {err:?}");
            }
        }
    }
}
