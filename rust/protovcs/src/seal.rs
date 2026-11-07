//! Naming chunks and protecting their contents.

use std::sync::Arc;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::error::{Error, ErrorKind, Result};
use crate::types::{CHUNK_ID_LEN, ChunkId, FORMAT_VERSION, Role};
use crate::wire::put_uvarint;
use crate::{MaybeSend, MaybeSync};

/// Names chunks and protects their contents.
///
/// [`Keyring`] is the real one. [`PlainSealer`] does neither, for local
/// development, tests and exports a person can read.
pub trait Sealer: MaybeSend + MaybeSync {
    /// Names a chunk. The same role, key and bytes always produce the same
    /// name.
    fn chunk_id(&self, role: Role, key: &str, canonical: &[u8]) -> ChunkId;

    /// Protects a chunk's contents, binding in its name.
    ///
    /// # Errors
    ///
    /// When no nonce can be drawn.
    fn seal(&self, id: &ChunkId, plaintext: &[u8]) -> Result<Vec<u8>>;

    /// Reverses [`Sealer::seal`].
    ///
    /// # Errors
    ///
    /// [`ErrorKind::UnsupportedFormat`] for a newer envelope and
    /// [`ErrorKind::Tampered`] for bytes that are not what `id` names.
    fn open(&self, id: &ChunkId, sealed: &[u8]) -> Result<Vec<u8>>;

    /// What a branch is stored under, so the chosen name stays private.
    fn ref_name(&self, branch: &str) -> Vec<u8>;

    /// Whether contents are protected.
    fn is_sealed(&self) -> bool;
}

/// Where random bytes come from: fills its argument or fails.
///
/// [`system_entropy`] everywhere except tests, which pass a fixed stream to
/// reproduce the golden vectors.
pub type Entropy = Arc<dyn Fn(&mut [u8]) -> Result<()> + Send + Sync>;

/// The operating system's random source, or `crypto.getRandomValues` in a
/// browser.
pub fn system_entropy() -> Entropy {
    Arc::new(|buf: &mut [u8]| {
        getrandom::getrandom(buf)
            .map_err(|e| Error::invalid(format!("protovcs: drawing random bytes: {e}")))
    })
}

const LABEL_NAME: &str = "protovcs/v1/chunk-name";
const LABEL_CHUNK: &str = "protovcs/v1/chunk-content";
const LABEL_REF: &str = "protovcs/v1/ref-name";

/// The length of the key a store is opened with.
pub const ROOT_KEY_LEN: usize = 32;

/// AES-GCM's nonce size.
const NONCE_LEN: usize = 12;

/// AES-GCM's tag size.
const TAG_LEN: usize = 16;

/// A store's keys, derived from its root key with HKDF-SHA256 and one label
/// per job: one key names chunks, one encrypts them, one names branches.
pub struct Keyring {
    name: [u8; 32],
    // Kept only for the golden vectors; the cipher holds its own copy.
    #[cfg(feature = "test-util")]
    chunk: [u8; 32],
    ref_key: [u8; 32],
    aead: Aes256Gcm,
    entropy: Entropy,
}

impl Keyring {
    /// Derives a store's keys from its root key, drawing nonces from the
    /// system's random source.
    ///
    /// # Errors
    ///
    /// When `root_key` is not [`ROOT_KEY_LEN`] bytes.
    pub fn new(root_key: &[u8]) -> Result<Self> {
        Self::build(root_key, system_entropy())
    }

    /// [`Keyring::new`] drawing nonces from `entropy`, for tests that need
    /// fixed nonces. A fixed nonce under a real key breaks AES-GCM, so this
    /// exists only with the `test-util` feature.
    ///
    /// # Errors
    ///
    /// When `root_key` is not [`ROOT_KEY_LEN`] bytes.
    #[cfg(feature = "test-util")]
    pub fn with_entropy(root_key: &[u8], entropy: Entropy) -> Result<Self> {
        Self::build(root_key, entropy)
    }

    fn build(root_key: &[u8], entropy: Entropy) -> Result<Self> {
        if root_key.len() != ROOT_KEY_LEN {
            return Err(Error::invalid(format!(
                "protovcs: a root key is {ROOT_KEY_LEN} bytes, got {}",
                root_key.len()
            )));
        }
        let hk = Hkdf::<Sha256>::new(None, root_key);
        let derive = |label: &str| {
            let mut out = [0u8; 32];
            // 32 bytes is far below HKDF-SHA256's limit of 8160.
            hk.expand(label.as_bytes(), &mut out)
                .expect("32 bytes is a valid HKDF-SHA256 output length");
            out
        };
        let chunk = derive(LABEL_CHUNK);
        let aead = Aes256Gcm::new_from_slice(&chunk).expect("a 32-byte key is a valid AES-256 key");
        Ok(Self {
            name: derive(LABEL_NAME),
            #[cfg(feature = "test-util")]
            chunk,
            ref_key: derive(LABEL_REF),
            aead,
            entropy,
        })
    }

    /// The derived naming, content and ref keys, for the golden vectors.
    #[cfg(feature = "test-util")]
    pub fn derived_keys(&self) -> (&[u8; 32], &[u8; 32], &[u8; 32]) {
        (&self.name, &self.chunk, &self.ref_key)
    }

    /// Seals with a chosen nonce, for the golden vectors.
    #[cfg(feature = "test-util")]
    pub fn seal_with_nonce(
        &self,
        id: &ChunkId,
        nonce: &[u8; NONCE_LEN],
        plaintext: &[u8],
    ) -> Vec<u8> {
        self.seal_nonce(id, nonce, plaintext)
    }

    fn seal_nonce(&self, id: &ChunkId, nonce: &[u8; NONCE_LEN], plaintext: &[u8]) -> Vec<u8> {
        let ct = self
            .aead
            .encrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: plaintext,
                    aad: id.as_bytes(),
                },
            )
            .expect("AES-GCM encryption does not fail for inputs of this size");
        let mut out = Vec::with_capacity(1 + NONCE_LEN + ct.len());
        out.push(FORMAT_VERSION);
        out.extend_from_slice(nonce);
        out.extend_from_slice(&ct);
        out
    }
}

impl Sealer for Keyring {
    fn chunk_id(&self, role: Role, key: &str, canonical: &[u8]) -> ChunkId {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.name)
            .expect("HMAC takes a key of any length");
        mac.update(&named(role, key));
        mac.update(canonical);
        let mut id = [0u8; CHUNK_ID_LEN];
        id.copy_from_slice(&mac.finalize().into_bytes());
        ChunkId::new(id)
    }

    fn seal(&self, id: &ChunkId, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; NONCE_LEN];
        (self.entropy)(&mut nonce)?;
        Ok(self.seal_nonce(id, &nonce, plaintext))
    }

    fn open(&self, id: &ChunkId, sealed: &[u8]) -> Result<Vec<u8>> {
        if sealed.len() < 1 + NONCE_LEN + TAG_LEN {
            return Err(Error::invalid(format!(
                "protovcs: chunk {id} is too short to be a sealed chunk"
            )));
        }
        if sealed[0] != FORMAT_VERSION {
            return Err(Error::new(
                ErrorKind::UnsupportedFormat,
                format!("chunk {id} is format {}", sealed[0]),
            ));
        }
        let nonce = &sealed[1..1 + NONCE_LEN];
        self.aead
            .decrypt(
                Nonce::from_slice(nonce),
                Payload {
                    msg: &sealed[1 + NONCE_LEN..],
                    aad: id.as_bytes(),
                },
            )
            .map_err(|_| Error::new(ErrorKind::Tampered, format!("chunk {id}")))
    }

    fn ref_name(&self, branch: &str) -> Vec<u8> {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.ref_key)
            .expect("HMAC takes a key of any length");
        mac.update(branch.as_bytes());
        mac.finalize().into_bytes().to_vec()
    }

    fn is_sealed(&self) -> bool {
        true
    }
}

/// Names chunks without a key and stores their contents as they are.
#[derive(Clone, Copy, Debug, Default)]
pub struct PlainSealer;

impl Sealer for PlainSealer {
    fn chunk_id(&self, role: Role, key: &str, canonical: &[u8]) -> ChunkId {
        let mut h = Sha256::new();
        h.update(named(role, key));
        h.update(canonical);
        ChunkId::new(h.finalize().into())
    }

    fn seal(&self, _id: &ChunkId, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(1 + plaintext.len());
        out.push(FORMAT_VERSION);
        out.extend_from_slice(plaintext);
        Ok(out)
    }

    fn open(&self, id: &ChunkId, sealed: &[u8]) -> Result<Vec<u8>> {
        match sealed.first() {
            None => Err(Error::invalid(format!("protovcs: chunk {id} is empty"))),
            Some(&v) if v != FORMAT_VERSION => Err(Error::new(
                ErrorKind::UnsupportedFormat,
                format!("chunk {id} is format {v}"),
            )),
            // Nothing here can detect a substitution, because nothing is
            // authenticated.
            Some(_) => Ok(sealed[1..].to_vec()),
        }
    }

    fn ref_name(&self, branch: &str) -> Vec<u8> {
        branch.as_bytes().to_vec()
    }

    fn is_sealed(&self) -> bool {
        false
    }
}

/// The role and key, each length-prefixed, so no two different pairs produce
/// the same input to the hash.
fn named(role: Role, key: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(role.as_str().len() + key.len() + 4);
    put_uvarint(&mut out, role.as_str().len() as u64);
    out.extend_from_slice(role.as_str().as_bytes());
    put_uvarint(&mut out, key.len() as u64);
    out.extend_from_slice(key.as_bytes());
    out
}

/// Reports whether `id` is the name of `canonical` under this role and key.
///
/// # Errors
///
/// [`ErrorKind::Tampered`] when it is not.
pub fn verify_chunk(
    s: &dyn Sealer,
    role: Role,
    key: &str,
    id: &ChunkId,
    canonical: &[u8],
) -> Result<()> {
    let want = s.chunk_id(role, key, canonical);
    let diff = want
        .as_bytes()
        .iter()
        .zip(id.as_bytes())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b));
    if diff != 0 {
        return Err(Error::new(
            ErrorKind::Tampered,
            format!("{id} holds the contents of {want}"),
        ));
    }
    Ok(())
}
