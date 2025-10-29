// In config/src/lib.rs

// Copyright(C) Facebook, Inc. and its affiliates.
use crypto::{
    generate_consensus_keypair, generate_production_keypair, ConsensusPublicKey,
    ConsensusSecretKey, PublicKey, SecretKey,
};
use log::{info, warn};
use rand::SeedableRng;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;
use std::net::SocketAddr;
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
    pub p2p_address: String,
}

#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Committee {
    pub authorities: BTreeMap<PublicKey, Authority>,
    pub epoch: u64,
}

// SỬA LỖI: Thêm dòng này để fix lỗi biên dịch
impl Import for Committee {}
impl Export for Committee {}

impl Committee {
    pub fn from_validator_info(
        mut validator_info: ValidatorInfo,
        self_address: &str,
        epoch: u64,
    ) -> Result<Self, ConfigError> {
        const BASE_PRIMARY_TO_WORKER_PORT: u16 = 10000;
        const BASE_WORKER_TO_PRIMARY_PORT: u16 = 11000;
        const BASE_TRANSACTIONS_PORT: u16 = 12000;

        validator_info
            .validators
            .sort_by(|a, b| a.address.cmp(&b.address));

        let mut authorities = BTreeMap::new();
        for (i, val) in validator_info.validators.iter().enumerate() {
            let base64_address = crypto::hex_to_base64(&val.pubkey_secp).map_err(|e| {
                ConfigError::ParseError(format!("Failed to convert hex address to base64: {}", e))
            })?;
            let public_key = PublicKey::decode_base64(&base64_address).map_err(|e| {
                ConfigError::ParseError(format!(
                    "Failed to parse public key '{}': {}",
                    val.address, e
                ))
            })?;

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

            let stake: Stake = val.total_staked_amount.parse().unwrap_or(1);
            let primary_to_primary: SocketAddr = val.primary_address.parse().map_err(|e| {
                ConfigError::ParseError(format!(
                    "Invalid primary_address '{}': {}",
                    val.primary_address, e
                ))
            })?;

            // DEBUG: Log primary address để trace
            if primary_to_primary.ip().to_string() == "0.0.0.0" || primary_to_primary.port() == 0 {
                warn!("[Config] ⚠️ INVALID PRIMARY ADDRESS for validator {} (address: {}, pubkey_secp: {}): {}", 
                      val.address, val.address, &val.pubkey_secp[..20], primary_to_primary);
            } else {
                info!(
                    "[Config] Loaded validator {} with primary_address: {}",
                    val.address, primary_to_primary
                );
            }

            let (worker_to_primary, _primary_to_worker, transactions) =
                if val.address.to_lowercase() == self_address.to_lowercase() {
                    info!(
                        "Assigning sequential internal ports for self (address: {}).",
                        self_address
                    );
                    (
                        format!("127.0.0.1:{}", BASE_WORKER_TO_PRIMARY_PORT + i as u16)
                            .parse()
                            .unwrap(),
                        format!("127.0.0.1:{}", BASE_PRIMARY_TO_WORKER_PORT + i as u16)
                            .parse()
                            .unwrap(),
                        format!("127.0.0.1:{}", BASE_TRANSACTIONS_PORT + i as u16)
                            .parse()
                            .unwrap(),
                    )
                } else {
                    let placeholder_addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
                    (placeholder_addr, placeholder_addr, placeholder_addr)
                };
            let primary_to_worker_2: SocketAddr = val.p2p_address.clone().parse().unwrap();

            let workers = [(
                0,
                WorkerAddresses {
                    primary_to_worker: primary_to_worker_2,
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
                p2p_address: val.p2p_address.clone(),
            };
            authorities.insert(public_key, authority);
        }
        Ok(Committee { authorities, epoch })
    }

    // ... (các hàm còn lại của Committee giữ nguyên) ...
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

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct NodeConfig {
    pub name: PublicKey,
    pub secret: SecretKey,
    pub consensus_key: ConsensusPublicKey,
    pub consensus_secret: ConsensusSecretKey,
    pub uds_get_validators_path: String,
    pub uds_block_path: String,
}

impl Import for NodeConfig {}
impl Export for NodeConfig {}

impl NodeConfig {
    pub fn new() -> Self {
        let (name, secret) = generate_production_keypair();
        let mut rng = rand::rngs::StdRng::from_entropy();
        let (consensus_key, consensus_secret) = generate_consensus_keypair(&mut rng);
        Self {
            name,
            secret,
            consensus_key,
            consensus_secret,
            uds_get_validators_path: "/tmp/get_validator.sock_1".to_string(),
            uds_block_path: "/tmp/block.sock_1".to_string(),
        }
    }
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self::new()
    }
}
