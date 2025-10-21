// crypto/src/lib.rs

// Copyright(C) Facebook, Inc. and its affiliates.

use fastcrypto::secp256k1::{Secp256k1KeyPair, Secp256k1PrivateKey};
use k256::elliptic_curve::sec1::ToEncodedPoint;
use k256::PublicKey as K256PublicKey;

// --- PHẦN IMPORT MỚI CHO BLS12-381 (ĐỒNG THUẬN) ---
use fastcrypto::bls12381::min_pk::{
    BLS12381AggregateSignature, BLS12381KeyPair, BLS12381PrivateKey, BLS12381PublicKey,
    BLS12381Signature,
};
use fastcrypto::error::FastCryptoError;
use fastcrypto::traits::{AggregateAuthenticator, Signer};

use anyhow::{Context, Result};
use base64;
use fastcrypto::encoding::{Base64, Encoding};
use fastcrypto::traits::{AllowedRng, KeyPair, ToFromBytes, VerifyingKey};
use hex;
use rand::SeedableRng;
use rand::{CryptoRng, RngCore};
use serde::Serialize;
use serde::{de, ser, Deserialize};
use sha3::{Digest as Sha3Digest, Keccak256};
use std::array::TryFromSliceError;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

pub fn generate_production_keypair() -> (PublicKey, SecretKey) {
    let mut rng = rand::rngs::StdRng::from_entropy();
    generate_keypair(&mut rng)
}

#[cfg(test)]
#[path = "tests/crypto_tests.rs"]
pub mod crypto_tests;

pub type CryptoError = FastCryptoError;

// Digest và Hash trait giữ nguyên
#[derive(
    Hash, PartialEq, Default, Eq, Clone, serde::Serialize, serde::Deserialize, Ord, PartialOrd,
)]
pub struct Digest(pub [u8; 32]);
/* ... implementation của Digest giữ nguyên ... */
impl Digest {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.to_vec()
    }
    pub fn size(&self) -> usize {
        self.0.len()
    }
}
impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", Base64::encode(&self.0))
    }
}
impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", Base64::encode(&self.0).get(0..16).unwrap_or(""))
    }
}
impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl TryFrom<&[u8]> for Digest {
    type Error = TryFromSliceError;
    fn try_from(item: &[u8]) -> Result<Self, Self::Error> {
        Ok(Digest(item.try_into()?))
    }
}
pub trait Hash {
    fn digest(&self) -> Digest;
}

// #################################################################
// ### PHẦN 1: ĐỊNH DANH NETWORK (SECP256K1) - GIỮ NGUYÊN       ###
// #################################################################

/// Represents a public key (in bytes) used for network identity.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct PublicKey(pub [u8; 33]);
/* ... implementation của PublicKey giữ nguyên ... */
impl Default for PublicKey {
    fn default() -> Self {
        PublicKey([0u8; 33])
    }
}
impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}
impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64().get(0..16).unwrap_or(""))
    }
}
impl PublicKey {
    pub fn encode_base64(&self) -> String {
        Base64::encode(&self.0[..])
    }
    pub fn decode_base64(s: &str) -> Result<Self, FastCryptoError> {
        let bytes = Base64::decode(s)?;
        let array = bytes[..33]
            .try_into()
            .map_err(|_| FastCryptoError::InvalidInput)?;
        Ok(Self(array))
    }

    pub fn to_eth_address(&self) -> String {
        let k256_public_key =
            K256PublicKey::from_sec1_bytes(&self.0).expect("Invalid public key bytes");
        let uncompressed_bytes = k256_public_key.to_encoded_point(false);
        let hash = Keccak256::digest(&uncompressed_bytes.as_bytes()[1..]);
        let address_bytes = &hash[12..];
        format!("0x{}", hex::encode(address_bytes))
    }
}
impl ser::Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}
impl<'de> de::Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))
    }
}
impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub struct SecretKey(Secp256k1PrivateKey);

impl Clone for SecretKey {
    fn clone(&self) -> Self {
        let bytes = self.0.as_ref();
        let private_key = Secp256k1PrivateKey::from_bytes(bytes).unwrap();
        Self(private_key)
    }
}

// --- ADD THIS DEBUG IMPLEMENTATION ---
impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}

impl fmt::Display for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64().get(0..16).unwrap_or(""))
    }
}
impl SecretKey {
    pub fn encode_base64(&self) -> String {
        Base64::encode(self.0.as_ref())
    }
    pub fn decode_base64(s: &str) -> Result<Self, FastCryptoError> {
        let bytes = Base64::decode(s)?;
        let private_key =
            Secp256k1PrivateKey::from_bytes(&bytes).map_err(|_| FastCryptoError::InvalidInput)?;
        Ok(Self(private_key))
    }

    pub fn to_public_key(&self) -> PublicKey {
        let private_key_bytes = self.0.as_ref();
        let sk = Secp256k1PrivateKey::from_bytes(private_key_bytes).unwrap();
        let keypair = Secp256k1KeyPair::from(sk);

        let public_bytes: [u8; 33] = keypair.public().as_ref().try_into().unwrap();
        PublicKey(public_bytes)
    }

    pub fn to_eth_address(&self) -> String {
        self.to_public_key().to_eth_address()
    }
}
impl ser::Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}
impl<'de> de::Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))
    }
}

/// Generates a keypair for network identity (secp256k1).
pub fn generate_keypair<R>(csprng: &mut R) -> (PublicKey, SecretKey)
where
    R: CryptoRng + RngCore + AllowedRng,
{
    let keypair = Secp256k1KeyPair::generate(csprng);
    let public_bytes: [u8; 33] = keypair.public().as_ref().try_into().unwrap();
    let public = PublicKey(public_bytes);
    let secret = SecretKey(keypair.private());
    (public, secret)
}

// ##################################################################
// ### PHẦN 2: ĐỒNG THUẬN (BLS12-381) - THAY ĐỔI VÀ BỔ SUNG      ###
// ##################################################################

// --- CẶP KHÓA BLS CHO ĐỒNG THUẬN ---

/// Represents a BLS public key used for consensus.

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize)] // <-- THÊM Serialize
pub struct ConsensusPublicKey(BLS12381PublicKey);

impl fmt::Display for ConsensusPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl ConsensusPublicKey {
    pub fn decode_base64(s: &str) -> Result<Self, FastCryptoError> {
        let bytes = Base64::decode(s)?;
        let public_key =
            BLS12381PublicKey::from_bytes(&bytes).map_err(|_| FastCryptoError::InvalidInput)?;
        Ok(Self(public_key))
    }
}

#[derive(Debug)]

pub struct ConsensusSecretKey(BLS12381PrivateKey);

// Implement Clone manually by reconstructing the key from its bytes.
impl Clone for ConsensusSecretKey {
    fn clone(&self) -> Self {
        let bytes = self.0.as_ref();
        let private_key =
            BLS12381PrivateKey::from_bytes(bytes).expect("Cloning a private key should not fail");
        Self(private_key)
    }
}

impl ConsensusSecretKey {
    pub fn encode_base64(&self) -> String {
        Base64::encode(self.0.as_ref())
    }
}
// Manual implementation of Serialize and Deserialize for ConsensusSecretKey
impl ser::Serialize for ConsensusSecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&Base64::encode(self.0.as_ref()))
    }
}

impl<'de> de::Deserialize<'de> for ConsensusSecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let bytes = Base64::decode(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        let private_key =
            BLS12381PrivateKey::from_bytes(&bytes).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(Self(private_key))
    }
}

/// Generates a keypair for consensus (BLS).
pub fn generate_consensus_keypair<R>(csprng: &mut R) -> (ConsensusPublicKey, ConsensusSecretKey)
where
    R: CryptoRng + RngCore + AllowedRng,
{
    let keypair = BLS12381KeyPair::generate(csprng);
    let public = ConsensusPublicKey(keypair.public().clone());
    let secret = ConsensusSecretKey(keypair.private());
    (public, secret)
}

// --- CHỮ KÝ BLS CHO ĐỒNG THUẬN ---

/// Represents a BLS signature.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Default, Serialize)]
pub struct Signature(BLS12381Signature);

impl Signature {
    /// Create a new BLS signature.
    pub fn new(digest: &Digest, secret: &ConsensusSecretKey) -> Self {
        let signature = secret.0.sign(digest.as_ref());
        Self(signature)
    }

    /// Verify a single BLS signature.
    pub fn verify(
        &self,
        digest: &Digest,
        public_key: &ConsensusPublicKey,
    ) -> Result<(), CryptoError> {
        public_key.0.verify(digest.as_ref(), &self.0)
    }

    /// Aggregate multiple signatures and verify them as a single batch.
    pub fn verify_batch<'a, I>(digest: &Digest, votes: I) -> Result<(), CryptoError>
    where
        I: IntoIterator<Item = &'a (ConsensusPublicKey, Signature)>,
    {
        let (pks, sigs): (Vec<_>, Vec<_>) = votes
            .into_iter()
            .map(|(pk, sig)| (pk.0.clone(), &sig.0)) // Clone the public key here
            .unzip();

        if pks.is_empty() {
            return Ok(());
        }

        // THAY ĐỔI Ở ĐÂY: Sử dụng BLS12381AggregateSignature để tổng hợp
        let aggregated_signature = BLS12381AggregateSignature::aggregate(sigs)?;

        // Xác minh chữ ký đã tổng hợp
        aggregated_signature.verify(pks.as_slice(), digest.as_ref())
    }
}

// --- DỊCH VỤ KÝ (SIGNATURE SERVICE) DÙNG BLS ---

#[derive(Clone)]
pub struct SignatureService {
    channel: Sender<(Digest, oneshot::Sender<Signature>)>,
}

impl SignatureService {
    pub fn new(secret: ConsensusSecretKey) -> Self {
        let (tx, mut rx): (Sender<(_, oneshot::Sender<_>)>, _) = channel(100);
        tokio::spawn(async move {
            while let Some((digest, sender)) = rx.recv().await {
                // Secret key is cloned for each signature request inside this task.
                let signature = Signature::new(&digest, &secret);
                let _ = sender.send(signature);
            }
        });
        Self { channel: tx }
    }

    pub async fn request_signature(&mut self, digest: Digest) -> Signature {
        let (sender, receiver): (oneshot::Sender<_>, oneshot::Receiver<_>) = oneshot::channel();
        if let Err(e) = self.channel.send((digest, sender)).await {
            panic!("Failed to send message to Signature Service: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive signature from Signature Service")
    }
}

pub fn hex_to_base64(hex_string: &str) -> Result<String> {
    // 1. Giải mã chuỗi hex thành các bytes
    let hex_no_prefix = hex_string.strip_prefix("0x").unwrap_or(&hex_string);
    let bytes = hex::decode(hex_no_prefix)
        .context(format!("Failed to decode hex string: '{}'", hex_no_prefix))?;

    // 2. Mã hóa các bytes thành chuỗi base64
    let base64_string = base64::encode(&bytes);

    Ok(base64_string)
}

pub fn base64_to_hex(base64_string: &str) -> Result<String> {
    // 1. Giải mã chuỗi base64 thành các bytes
    let bytes = base64::decode(base64_string).context(format!(
        "Failed to decode base64 string: '{}'",
        base64_string
    ))?;

    // 2. Mã hóa các bytes thành chuỗi hex
    let hex_string = hex::encode(&bytes);

    Ok(hex_string)
}
