// Copyright(C) Facebook, Inc. and its affiliates.

// --- ĐÃ CẬP NHẬT TOÀN BỘ IMPORT ---
use fastcrypto::secp256k1::{
    Secp256k1KeyPair, Secp256k1PublicKey, Secp256k1PrivateKey, Secp256k1Signature,
};
use fastcrypto::traits::{KeyPair, RecoverableSigner, Signer, ToFromBytes, VerifyingKey, AllowedRng};
use fastcrypto::encoding::{Base64, Encoding};
use fastcrypto::error::FastCryptoError;
use rand::SeedableRng;
use rand::{CryptoRng, RngCore};
use serde::{de, ser, Deserializer, Serializer, Deserialize};
use std::array::TryFromSliceError;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;
use sha3::Digest as Sha3Digest;

#[cfg(test)]
#[path = "tests/crypto_tests.rs"]
pub mod crypto_tests;

pub type CryptoError = FastCryptoError;

/// Represents a Keccak-256 hash digest (32 bytes).
#[derive(Hash, PartialEq, Default, Eq, Clone, serde::Serialize, serde::Deserialize, Ord, PartialOrd)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub fn to_vec(&self) -> Vec<u8> { self.0.to_vec() }
    pub fn size(&self) -> usize { self.0.len() }
}
impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> { write!(f, "{}", Base64::encode(&self.0)) }
}
impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> { write!(f, "{}", Base64::encode(&self.0).get(0..16).unwrap_or("")) }
}
impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] { &self.0 }
}
impl TryFrom<&[u8]> for Digest {
    type Error = TryFromSliceError;
    fn try_from(item: &[u8]) -> Result<Self, Self::Error> { Ok(Digest(item.try_into()?)) }
}

pub trait Hash {
    fn digest(&self) -> Digest;
}

/// Represents a public key (in bytes).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct PublicKey(pub [u8; 33]);

impl Default for PublicKey {
    fn default() -> Self { PublicKey([0u8; 33]) }
}
impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> { write!(f, "{}", self.encode_base64()) }
}
impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> { write!(f, "{}", self.encode_base64().get(0..16).unwrap_or("")) }
}
impl PublicKey {
    pub fn encode_base64(&self) -> String { Base64::encode(&self.0[..]) }
    pub fn decode_base64(s: &str) -> Result<Self, FastCryptoError> {
        let bytes = Base64::decode(s)?;
        let array = bytes[..33].try_into().map_err(|_| FastCryptoError::InvalidInput)?;
        Ok(Self(array))
    }
}
impl ser::Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: ser::Serializer {
        serializer.serialize_str(&self.encode_base64())
    }
}
impl<'de> de::Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: de::Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))
    }
}
impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] { &self.0 }
}

/// Represents a secret key.
pub struct SecretKey(Secp256k1PrivateKey);

// --- SỬA LỖI TRAIT: Triển khai Clone thủ công cho SecretKey ---
impl Clone for SecretKey {
    fn clone(&self) -> Self {
        let bytes = self.0.as_ref();
        let private_key = Secp256k1PrivateKey::from_bytes(bytes).unwrap();
        Self(private_key)
    }
}

impl SecretKey {
    pub fn encode_base64(&self) -> String { Base64::encode(self.0.as_ref()) }
    pub fn decode_base64(s: &str) -> Result<Self, FastCryptoError> {
        let bytes = Base64::decode(s)?;
        let private_key = Secp256k1PrivateKey::from_bytes(&bytes).map_err(|_| FastCryptoError::InvalidInput)?;
        Ok(Self(private_key))
    }
}
impl ser::Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: ser::Serializer {
        serializer.serialize_str(&self.encode_base64())
    }
}
impl<'de> de::Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: de::Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))
    }
}

pub fn generate_production_keypair() -> (PublicKey, SecretKey) {
    let mut rng = rand::rngs::StdRng::from_entropy();
    generate_keypair(&mut rng)
}

pub fn generate_keypair<R>(csprng: &mut R) -> (PublicKey, SecretKey) where R: CryptoRng + RngCore + AllowedRng {
    let keypair = Secp256k1KeyPair::generate(csprng);
    let public_bytes: [u8; 33] = keypair.public().as_ref().try_into().unwrap();
    let public = PublicKey(public_bytes);
    let secret = SecretKey(keypair.private());
    (public, secret)
}

/// Represents a recoverable secp256k1 signature held as a byte array.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Signature(pub [u8; 65]);

impl Default for Signature {
    fn default() -> Self { Signature([0u8; 65]) }
}
impl ser::Serialize for Signature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
        serializer.serialize_bytes(&self.0)
    }
}
impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error> where D: Deserializer<'de> {
        // Sử dụng một visitor để deserialize mảng byte một cách an toàn
        struct ByteArrayVisitor;
        impl<'de> de::Visitor<'de> for ByteArrayVisitor {
            type Value = [u8; 65];
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a byte array of length 65")
            }
            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E> where E: de::Error {
                v.try_into().map_err(|_| E::invalid_length(v.len(), &self))
            }
        }
        let array = deserializer.deserialize_bytes(ByteArrayVisitor)?;
        Ok(Signature(array))
    }
}

impl Signature {
    pub fn new(digest: &Digest, secret: &SecretKey) -> Self {
        // Clone toàn bộ SecretKey, sau đó lấy Secp256k1PrivateKey bên trong
        let keypair = Secp256k1KeyPair::from(secret.clone().0);
        let signature_internal = keypair.sign_recoverable(digest.as_ref());
        let mut sig_bytes = [0u8; 65];
        sig_bytes.copy_from_slice(signature_internal.as_ref());
        Self(sig_bytes)
    }

    pub fn verify(&self, digest: &Digest, public_key: &PublicKey) -> Result<(), CryptoError> {
        let non_recoverable_sig_bytes: &[u8; 64] = self.0[..64].try_into().unwrap();
        let non_recoverable_signature = Secp256k1Signature::from_bytes(non_recoverable_sig_bytes)
            .map_err(|_| FastCryptoError::InvalidInput)?;
        let fc_public_key = Secp256k1PublicKey::from_bytes(&public_key.0)
            .map_err(|_| FastCryptoError::InvalidInput)?;
        fc_public_key.verify(digest.as_ref(), &non_recoverable_signature)
    }

    pub fn verify_batch<'a, I>(digest: &Digest, votes: I) -> Result<(), CryptoError> where I: IntoIterator<Item = &'a (PublicKey, Signature)> {
        for (key, sig) in votes.into_iter() {
            sig.verify(digest, key)?;
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SignatureService {
    channel: Sender<(Digest, oneshot::Sender<Signature>)>,
}

impl SignatureService {
    pub fn new(secret: SecretKey) -> Self {
        let (tx, mut rx): (Sender<(_, oneshot::Sender<_>)>, _) = channel(100);
        tokio::spawn(async move {
            while let Some((digest, sender)) = rx.recv().await {
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
        receiver.await.expect("Failed to receive signature from Signature Service")
    }
}