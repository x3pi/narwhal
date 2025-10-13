// Copyright(C) Facebook, Inc. and its affiliates.
use fastcrypto::ed25519::{Ed25519KeyPair, Ed25519PublicKey, Ed25519PrivateKey, Ed25519Signature};
use fastcrypto::encoding::{Base64, Encoding};
use fastcrypto::traits::{KeyPair, Signer, ToFromBytes, VerifyingKey, AllowedRng};
use fastcrypto::error::FastCryptoError;
use rand::SeedableRng;
use rand::{CryptoRng, RngCore};
use serde::{de, ser, Deserialize, Serialize};
use std::array::TryFromSliceError;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/crypto_tests.rs"]
pub mod crypto_tests;

pub type CryptoError = FastCryptoError;

/// Represents a hash digest (32 bytes).
#[derive(Hash, PartialEq, Default, Eq, Clone, Deserialize, Serialize, Ord, PartialOrd)]
pub struct Digest(pub [u8; 32]);

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

/// This trait is implemented by all messages that can be hashed.
pub trait Hash {
    fn digest(&self) -> Digest;
}

/// Represents a public key (in bytes).
// KHẮC PHỤC: Derive Copy để tương thích ngược với config/src/lib.rs.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Default)]
pub struct PublicKey(pub [u8; 32]);

impl PublicKey {
    pub fn encode_base64(&self) -> String {
        Base64::encode(&self.0[..])
    }

    pub fn decode_base64(s: &str) -> Result<Self, FastCryptoError> {
        let bytes = Base64::decode(s)?;
        let array = bytes[..32]
            .try_into()
            .map_err(|_| FastCryptoError::InvalidInput)?;
        Ok(Self(array))
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

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Represents a secret key (in bytes).
pub struct SecretKey(Ed25519PrivateKey);

impl SecretKey {
    pub fn encode_base64(&self) -> String {
        Base64::encode(self.0.as_ref())
    }

    pub fn decode_base64(s: &str) -> Result<Self, FastCryptoError> {
        let bytes = Base64::decode(s)?;
        let key = Ed25519PrivateKey::from_bytes(&bytes)
            .map_err(|_| FastCryptoError::InvalidInput)?;
        Ok(Self(key))
    }
}

impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value = Self::decode_base64(&s).map_err(|e| de::Error::custom(e.to_string()))?;
        Ok(value)
    }
}

pub fn generate_production_keypair() -> (PublicKey, SecretKey) {
    let mut rng = rand::rngs::StdRng::from_entropy();
    generate_keypair(&mut rng)
}

pub fn generate_keypair<R>(csprng: &mut R) -> (PublicKey, SecretKey)
where
    R: CryptoRng + RngCore + AllowedRng,
{
    let keypair = Ed25519KeyPair::generate(csprng);
    
    // KHẮC PHỤC: Chuyển đổi Ed25519PublicKey sang [u8; 32] để tạo PublicKey (Copy).
    let public_bytes: [u8; 32] = keypair.public().as_ref().try_into().unwrap();
    let public = PublicKey(public_bytes);

    // Tạo SecretKey
    let private_key = keypair.private();
    let private_key_bytes = private_key.as_bytes();
    
    let secret_key = Ed25519PrivateKey::from_bytes(private_key_bytes).unwrap();
    let secret = SecretKey(secret_key);
    
    (public, secret)
}

/// Represents an ed25519 signature.
#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct Signature(pub Ed25519Signature);

impl Signature {
    pub fn new(digest: &Digest, secret: &SecretKey) -> Self {
        let keypair = Ed25519KeyPair::from_bytes(secret.0.as_bytes()).unwrap();
        let signature = keypair.sign(digest.as_ref());
        Self(signature)
    }

    pub fn verify(&self, digest: &Digest, public_key: &PublicKey) -> Result<(), CryptoError> {
        // KHẮC PHỤC: Chuyển đổi PublicKey (byte array) thành Ed25519PublicKey để verify.
        let fc_public_key = Ed25519PublicKey::from_bytes(&public_key.0)
            .map_err(|_| FastCryptoError::InvalidInput)?;
        fc_public_key.verify(digest.as_ref(), &self.0)
            .map_err(CryptoError::from)
    }

    pub fn verify_batch<'a, I>(digest: &Digest, votes: I) -> Result<(), CryptoError>
    where
        I: IntoIterator<Item = &'a (PublicKey, Signature)>,
    {
        let mut verifications = Vec::new();
        for (key, sig) in votes.into_iter() {
            // KHẮC PHỤC: Chuyển đổi PublicKey (byte array) thành Ed25519PublicKey để verify.
            let fc_key = Ed25519PublicKey::from_bytes(&key.0)
                .map_err(|_| FastCryptoError::InvalidInput)?;
            verifications.push(fc_key.verify(digest.as_ref(), &sig.0));
        }

        verifications.into_iter().collect::<Result<Vec<_>, _>>()
            .map_err(CryptoError::from)?;
        Ok(())
    }
}

/// This service holds the node's private key. It takes digests as input and returns a signature
/// over the digest (through a oneshot channel).
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
            panic!("Failed to send message Signature Service: {}", e);
        }
        receiver
            .await
            .expect("Failed to receive signature from Signature Service")
    }
}