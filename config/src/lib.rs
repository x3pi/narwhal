// In config/src/lib.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crypto::{
    generate_consensus_keypair, generate_production_keypair, ConsensusPublicKey,
    ConsensusSecretKey, PublicKey, SecretKey,
};
use log::info;
use rand::{RngCore, SeedableRng}; // <-- Thêm RngCore
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256}; // <-- Thêm các import cho sha3
use std::collections::{BTreeMap, HashMap};
use std::convert::TryFrom;
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
use std::str::FromStr;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Node {0} is not in the committee")]
    NotInCommittee(PublicKey),

    #[error("Unknown worker id {0}")]
    UnknownWorker(WorkerId),

    #[error("Failed to read config file '{file}': {message}")]
    ImportError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    ExportError { file: String, message: String },

    #[error("Failed to parse validator data: {0}")]
    ParseError(String),
}

// SỬA ĐỔI: Thêm Serialize và Deserialize để có thể import/export
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Validator {
    pub address: String,
    pub primary_address: String,
    pub worker_address: String,
    pub p2p_address: String,
    pub total_staked_amount: String,
    pub pubkey_bls: String,
    pub pubkey_secp: String,
}

/// Struct wrapper containing a list of validators.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidatorInfo {
    pub validators: Vec<Validator>,
}

pub trait Import: DeserializeOwned {
    fn import(path: &str) -> Result<Self, ConfigError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConfigError::ImportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub trait Export: Serialize {
    fn export(&self, path: &str) -> Result<(), ConfigError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConfigError::ExportError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

pub type Stake = u32;
pub type WorkerId = u32;

#[derive(Clone, Deserialize, Serialize)]
pub struct Parameters {
    pub header_size: usize,
    pub max_header_delay: u64,
    pub gc_depth: u64,
    pub sync_retry_delay: u64,
    pub sync_retry_nodes: usize,
    pub batch_size: usize,
    pub max_batch_delay: u64,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            header_size: 1_000,
            max_header_delay: 100,
            gc_depth: 50,
            sync_retry_delay: 5_000,
            sync_retry_nodes: 3,
            batch_size: 500_000,
            max_batch_delay: 100,
        }
    }
}

impl Import for Parameters {}
impl Export for Parameters {}

impl Parameters {
    pub fn log(&self) {
        info!("Header size set to {} B", self.header_size);
        info!("Max header delay set to {} ms", self.max_header_delay);
        info!("Garbage collection depth set to {} rounds", self.gc_depth);
        info!("Sync retry delay set to {} ms", self.sync_retry_delay);
        info!("Sync retry nodes set to {} nodes", self.sync_retry_nodes);
        info!("Batch size set to {} B", self.batch_size);
        info!("Max batch delay set to {} ms", self.max_batch_delay);
    }
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct PrimaryAddresses {
    pub primary_to_primary: SocketAddr,
    pub worker_to_primary: SocketAddr,
}

#[derive(Clone, Deserialize, Eq, Hash, PartialEq, Serialize, Debug)]
pub struct WorkerAddresses {
    pub transactions: SocketAddr,
    pub worker_to_worker: SocketAddr,
    pub primary_to_worker: SocketAddr,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Authority {
    pub stake: Stake,
    pub consensus_key: ConsensusPublicKey,
    pub primary: PrimaryAddresses,
    pub workers: HashMap<WorkerId, WorkerAddresses>,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Committee {
    pub authorities: BTreeMap<PublicKey, Authority>,
}

impl Committee {
    // Thêm tham số `self_address` để xác định node hiện tại.
    pub fn from_validator_info(
        mut validator_info: ValidatorInfo,
        self_address: &str,
    ) -> Result<Self, ConfigError> {
        const BASE_PRIMARY_TO_WORKER_PORT: u16 = 10000;
        const BASE_WORKER_TO_PRIMARY_PORT: u16 = 11000;
        const BASE_TRANSACTIONS_PORT: u16 = 12000;

        validator_info
            .validators
            .sort_by(|a, b| a.address.cmp(&b.address));

        let mut authorities = BTreeMap::new();
        for (i, val) in validator_info.validators.iter().enumerate() {
            // Parse a validator's secp256k1 public key.
            let base64_address = crypto::hex_to_base64(&val.pubkey_secp).map_err(|e| {
                ConfigError::ParseError(format!("Failed to convert hex address to base64: {}", e))
            })?;
            let public_key = PublicKey::decode_base64(&base64_address).map_err(|e| {
                ConfigError::ParseError(format!(
                    "Failed to parse public key '{}': {}",
                    val.address, e
                ))
            })?;

            // Parse the consensus public key (bls).
            let base64_bls_key = crypto::hex_to_base64(&val.pubkey_bls).map_err(|e| {
                ConfigError::ParseError(format!("Failed to convert BLS hex key to base64: {}", e))
            })?;
            let consensus_key =
                ConsensusPublicKey::decode_base64(&base64_bls_key).map_err(|e| {
                    ConfigError::ParseError(format!(
                        "Failed to parse consensus key '{}': {}",
                        val.pubkey_bls, e
                    ))
                })?;

            // Parse other validator info.
            let stake: Stake = val.total_staked_amount.parse().unwrap_or(1);
            let primary_to_primary: SocketAddr = val.primary_address.parse().map_err(|e| {
                ConfigError::ParseError(format!(
                    "Invalid primary_address '{}': {}",
                    val.primary_address, e
                ))
            })?;

            // --- LOGIC GÁN ĐỊA CHỈ NỘI BỘ ---
            let worker_to_primary: SocketAddr;
            let primary_to_worker: SocketAddr;
            let transactions: SocketAddr;

            // Kiểm tra xem validator đang xử lý có phải là chính node này không.
            info!(
                "Assigning sequential internal ports for self (address: {}  :: {}).",
                self_address, val.address
            );
            if val.address.to_lowercase() == self_address.to_lowercase() {
                // Nếu ĐÚNG, tự động gán cổng tuần tự cho chính nó.
                info!(
                    "Assigning sequential internal ports for self (address: {}).",
                    self_address
                );
                worker_to_primary = format!("127.0.0.1:{}", BASE_WORKER_TO_PRIMARY_PORT + i as u16)
                    .parse()
                    .unwrap();
                primary_to_worker = format!("127.0.0.1:{}", BASE_PRIMARY_TO_WORKER_PORT + i as u16)
                    .parse()
                    .unwrap();
                transactions = format!("127.0.0.1:{}", BASE_TRANSACTIONS_PORT + i as u16)
                    .parse()
                    .unwrap();
            } else {
                // Nếu KHÔNG, gán địa chỉ placeholder "0.0.0.0:0" vì không cần thiết.
                // let placeholder_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
                // worker_to_primary = placeholder_addr;
                // primary_to_worker = placeholder_addr;
                // transactions = placeholder_addr;
                transactions = format!("127.0.0.1:{}", BASE_TRANSACTIONS_PORT + i as u16)
                    .parse()
                    .unwrap();
                worker_to_primary = format!("127.0.0.1:{}", BASE_WORKER_TO_PRIMARY_PORT + i as u16)
                    .parse()
                    .unwrap();
                primary_to_worker = format!("127.0.0.1:{}", BASE_PRIMARY_TO_WORKER_PORT + i as u16)
                    .parse()
                    .unwrap();
            }
            // --- KẾT THÚC LOGIC GÁN ĐỊA CHỈ ---

            let workers = [(
                0, // Giả sử mỗi validator chỉ có một worker với id = 0.
                WorkerAddresses {
                    primary_to_worker,
                    transactions,
                    worker_to_worker: val.worker_address.parse().map_err(|e| {
                        ConfigError::ParseError(format!(
                            "Invalid worker_address '{}': {}",
                            val.worker_address, e
                        ))
                    })?,
                },
            )]
            .iter()
            .cloned()
            .collect();

            let authority = Authority {
                stake,
                consensus_key,
                primary: PrimaryAddresses {
                    primary_to_primary,
                    worker_to_primary,
                },
                workers,
            };
            authorities.insert(public_key, authority);
        }
        Ok(Committee { authorities })
    }
}

impl Import for Committee {}
impl Export for Committee {}

impl Committee {
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(name).map_or_else(|| 0, |x| x.stake)
    }

    pub fn consensus_key(&self, name: &PublicKey) -> Option<ConsensusPublicKey> {
        self.authorities.get(name).map(|x| x.consensus_key.clone())
    }

    pub fn others_stake(&self, myself: &PublicKey) -> Vec<(PublicKey, Stake)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.stake))
            .collect()
    }

    pub fn quorum_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    pub fn validity_threshold(&self) -> Stake {
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        (total_votes + 2) / 3
    }

    pub fn primary(&self, to: &PublicKey) -> Result<PrimaryAddresses, ConfigError> {
        self.authorities
            .get(to)
            .map(|x| x.primary.clone())
            .ok_or_else(|| ConfigError::NotInCommittee(*to))
    }

    pub fn others_primaries(&self, myself: &PublicKey) -> Vec<(PublicKey, PrimaryAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .map(|(name, authority)| (*name, authority.primary.clone()))
            .collect()
    }

    pub fn worker(&self, to: &PublicKey, id: &WorkerId) -> Result<WorkerAddresses, ConfigError> {
        self.authorities
            .iter()
            .find(|(name, _)| name == &to)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*to))?
            .workers
            .iter()
            .find(|(worker_id, _)| worker_id == &id)
            .map(|(_, worker)| worker.clone())
            .ok_or_else(|| ConfigError::UnknownWorker(*id))
    }

    pub fn our_workers(&self, myself: &PublicKey) -> Result<Vec<WorkerAddresses>, ConfigError> {
        Ok(self
            .authorities
            .iter()
            .find(|(name, _)| name == &myself)
            .map(|(_, authority)| authority)
            .ok_or_else(|| ConfigError::NotInCommittee(*myself))?
            .workers
            .values()
            .cloned()
            .collect())
    }

    pub fn others_workers(
        &self,
        myself: &PublicKey,
        id: &WorkerId,
    ) -> Vec<(PublicKey, WorkerAddresses)> {
        self.authorities
            .iter()
            .filter(|(name, _)| name != &myself)
            .filter_map(|(name, authority)| {
                authority
                    .workers
                    .iter()
                    .find(|(worker_id, _)| worker_id == &id)
                    .map(|(_, addresses)| (*name, addresses.clone()))
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize)]
pub struct KeyPair {
    pub name: PublicKey,
    pub secret: SecretKey,
    pub consensus_key: ConsensusPublicKey,
    pub consensus_secret: ConsensusSecretKey,
}

impl Import for KeyPair {}
impl Export for KeyPair {}

impl KeyPair {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        let mut rng = rand::rngs::StdRng::from_entropy();
        let (consensus_key, consensus_secret) = generate_consensus_keypair(&mut rng);
        Self {
            name,
            secret,
            consensus_key,
            consensus_secret,
        }
    }
}

impl Default for KeyPair {
    fn default() -> Self {
        Self::new()
    }
}
