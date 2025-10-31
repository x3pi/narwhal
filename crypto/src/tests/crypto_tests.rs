// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use rand::rngs::StdRng;
use sha3::{Digest as Sha3DigestTrait, Sha3_512};

// ##################################################################
// ### Test Helpers                                               ###
// ##################################################################

impl Hash for &[u8] {
    fn digest(&self) -> Digest {
        let hash = Sha3_512::digest(self);
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&hash[..32]);
        Digest(bytes)
    }
}

// Custom PartialEq for SecretKey as it's a newtype around a type that doesn't derive it.
impl PartialEq for SecretKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref() == other.0.as_ref()
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.encode_base64())
    }
}

/// Fixture for generating network identity keypairs (secp256k1).
pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

/// Fixture for generating consensus keypairs (BLS12-381).
pub fn consensus_keys() -> Vec<(ConsensusPublicKey, ConsensusSecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4)
        .map(|_| generate_consensus_keypair(&mut rng))
        .collect()
}

// ##################################################################
// ### Network Identity Key Tests (secp256k1)                     ###
// ##################################################################

#[test]
fn import_export_public_key() {
    let (public_key, _) = keys().pop().unwrap();
    let export = public_key.encode_base64();
    let import = PublicKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), public_key);
}

#[test]
fn import_export_secret_key() {
    let (_, secret_key) = keys().pop().unwrap();
    let export = secret_key.encode_base64();
    let import = SecretKey::decode_base64(&export);
    assert!(import.is_ok());
    assert_eq!(import.unwrap(), secret_key);
}

// ##################################################################
// ### Consensus Key & Signature Tests (BLS12-381)                ###
// ##################################################################

#[test]
fn verify_valid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = consensus_keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature.
    assert!(signature.verify(&digest, &public_key).is_ok());
}

#[test]
fn verify_invalid_signature() {
    // Get a keypair.
    let (public_key, secret_key) = consensus_keys().pop().unwrap();

    // Make signature.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = Signature::new(&digest, &secret_key);

    // Verify the signature against a different message.
    let bad_message: &[u8] = b"Bad message!";
    let bad_digest = bad_message.digest();
    assert!(signature.verify(&bad_digest, &public_key).is_err());
}

#[test]
fn verify_valid_batch() {
    // Make signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let mut keys = consensus_keys();
    let signatures: Vec<_> = (0..3)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();

    // Verify the batch.
    assert!(Signature::verify_batch(&digest, &signatures).is_ok());
}

#[test]
fn verify_invalid_batch() {
    // Make 2 valid signatures.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let mut keys = consensus_keys();
    let mut signatures: Vec<_> = (0..2)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();

    // Add an invalid signature.
    let (public_key, _) = keys.pop().unwrap();
    signatures.push((public_key, Signature::default()));

    // Verify the batch.
    assert!(Signature::verify_batch(&digest, &signatures).is_err());
}

#[tokio::test]
async fn signature_service() {
    // Get a keypair.
    let (public_key, secret_key) = consensus_keys().pop().unwrap();

    // Spawn the signature service.
    let mut service = SignatureService::new(secret_key);

    // Request signature from the service.
    let message: &[u8] = b"Hello, world!";
    let digest = message.digest();
    let signature = service.request_signature(digest.clone()).await;

    // Verify the signature we received.
    assert!(signature.verify(&digest, &public_key).is_ok());
}
