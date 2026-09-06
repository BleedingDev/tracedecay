//! Canonical serde traversal and dependency-free SHA-256.

use serde::ser::{
    Error as SerError, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant,
    SerializeTuple, SerializeTupleStruct, SerializeTupleVariant,
};
use serde::Serialize;
use std::fmt;

pub(super) fn digest<T: Serialize>(value: &T) -> [u8; 32] {
    let mut encoder = Encoder::default();
    match value.serialize(&mut encoder) {
        Ok(()) => sha256(&encoder.bytes),
        Err(error) => sha256(error.to_string().as_bytes()),
    }
}

#[derive(Debug)]
struct EncodeError(String);

impl fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for EncodeError {}

impl SerError for EncodeError {
    fn custom<T: fmt::Display>(message: T) -> Self {
        Self(message.to_string())
    }
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn tag(&mut self, tag: u8) {
        self.bytes.push(tag);
    }

    fn length(&mut self, length: usize) {
        self.bytes.extend_from_slice(&(length as u64).to_be_bytes());
    }

    fn text(&mut self, value: &str) {
        self.length(value.len());
        self.bytes.extend_from_slice(value.as_bytes());
    }
}

struct Compound<'a> {
    encoder: &'a mut Encoder,
}

impl<'a> serde::Serializer for &'a mut Encoder {
    type Ok = ();
    type Error = EncodeError;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn serialize_bool(self, value: bool) -> Result<Self::Ok, Self::Error> {
        self.tag(1);
        self.tag(u8::from(value));
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
        self.tag(2);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
        self.tag(3);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
        self.tag(4);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
        self.tag(5);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_i128(self, value: i128) -> Result<Self::Ok, Self::Error> {
        self.tag(6);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
        self.tag(7);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
        self.tag(8);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
        self.tag(9);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
        self.tag(10);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_u128(self, value: u128) -> Result<Self::Ok, Self::Error> {
        self.tag(11);
        self.bytes.extend_from_slice(&value.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        self.tag(12);
        self.bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        Ok(())
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        self.tag(13);
        self.bytes.extend_from_slice(&value.to_bits().to_be_bytes());
        Ok(())
    }

    fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
        self.tag(14);
        self.bytes.extend_from_slice(&(value as u32).to_be_bytes());
        Ok(())
    }

    fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
        self.tag(15);
        self.text(value);
        Ok(())
    }

    fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.tag(16);
        self.length(value.len());
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.tag(17);
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        self.tag(18);
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.tag(19);
        Ok(())
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.tag(20);
        self.text(name);
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.tag(21);
        self.text(name);
        self.bytes.extend_from_slice(&variant_index.to_be_bytes());
        self.text(variant);
        Ok(())
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.tag(22);
        self.text(name);
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        self.tag(23);
        self.text(name);
        self.bytes.extend_from_slice(&variant_index.to_be_bytes());
        self.text(variant);
        value.serialize(self)
    }

    fn serialize_seq(self, length: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.tag(24);
        self.length(length.unwrap_or(usize::MAX));
        Ok(Compound { encoder: self })
    }

    fn serialize_tuple(self, length: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.tag(25);
        self.length(length);
        Ok(Compound { encoder: self })
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        length: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.tag(26);
        self.text(name);
        self.length(length);
        Ok(Compound { encoder: self })
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.tag(27);
        self.text(name);
        self.bytes.extend_from_slice(&variant_index.to_be_bytes());
        self.text(variant);
        self.length(length);
        Ok(Compound { encoder: self })
    }

    fn serialize_map(self, length: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.tag(28);
        self.length(length.unwrap_or(usize::MAX));
        Ok(Compound { encoder: self })
    }

    fn serialize_struct(
        self,
        name: &'static str,
        length: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.tag(29);
        self.text(name);
        self.length(length);
        Ok(Compound { encoder: self })
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        length: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.tag(30);
        self.text(name);
        self.bytes.extend_from_slice(&variant_index.to_be_bytes());
        self.text(variant);
        self.length(length);
        Ok(Compound { encoder: self })
    }

    fn is_human_readable(&self) -> bool {
        false
    }
}

impl SerializeSeq for Compound<'_> {
    type Ok = ();
    type Error = EncodeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), EncodeError> {
        value.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl SerializeTuple for Compound<'_> {
    type Ok = ();
    type Error = EncodeError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), EncodeError> {
        value.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl SerializeTupleStruct for Compound<'_> {
    type Ok = ();
    type Error = EncodeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), EncodeError> {
        value.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl SerializeTupleVariant for Compound<'_> {
    type Ok = ();
    type Error = EncodeError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), EncodeError> {
        value.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl SerializeMap for Compound<'_> {
    type Ok = ();
    type Error = EncodeError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), EncodeError> {
        key.serialize(&mut *self.encoder)
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), EncodeError> {
        value.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl SerializeStruct for Compound<'_> {
    type Ok = ();
    type Error = EncodeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), EncodeError> {
        self.encoder.text(key);
        value.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), EncodeError> {
        Ok(())
    }
}

impl SerializeStructVariant for Compound<'_> {
    type Ok = ();
    type Error = EncodeError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), EncodeError> {
        self.encoder.text(key);
        value.serialize(&mut *self.encoder)
    }

    fn end(self) -> Result<(), EncodeError> {
        Ok(())
    }
}

fn sha256(message: &[u8]) -> [u8; 32] {
    const INITIAL: [u32; 8] = [
        0x6a09_e667,
        0xbb67_ae85,
        0x3c6e_f372,
        0xa54f_f53a,
        0x510e_527f,
        0x9b05_688c,
        0x1f83_d9ab,
        0x5be0_cd19,
    ];
    const ROUND: [u32; 64] = [
        0x428a_2f98,
        0x7137_4491,
        0xb5c0_fbcf,
        0xe9b5_dba5,
        0x3956_c25b,
        0x59f1_11f1,
        0x923f_82a4,
        0xab1c_5ed5,
        0xd807_aa98,
        0x1283_5b01,
        0x2431_85be,
        0x550c_7dc3,
        0x72be_5d74,
        0x80de_b1fe,
        0x9bdc_06a7,
        0xc19b_f174,
        0xe49b_69c1,
        0xefbe_4786,
        0x0fc1_9dc6,
        0x240c_a1cc,
        0x2de9_2c6f,
        0x4a74_84aa,
        0x5cb0_a9dc,
        0x76f9_88da,
        0x983e_5152,
        0xa831_c66d,
        0xb003_27c8,
        0xbf59_7fc7,
        0xc6e0_0bf3,
        0xd5a7_9147,
        0x06ca_6351,
        0x1429_2967,
        0x27b7_0a85,
        0x2e1b_2138,
        0x4d2c_6dfc,
        0x5338_0d13,
        0x650a_7354,
        0x766a_0abb,
        0x81c2_c92e,
        0x9272_2c85,
        0xa2bf_e8a1,
        0xa81a_664b,
        0xc24b_8b70,
        0xc76c_51a3,
        0xd192_e819,
        0xd699_0624,
        0xf40e_3585,
        0x106a_a070,
        0x19a4_c116,
        0x1e37_6c08,
        0x2748_774c,
        0x34b0_bcb5,
        0x391c_0cb3,
        0x4ed8_aa4a,
        0x5b9c_ca4f,
        0x682e_6ff3,
        0x748f_82ee,
        0x78a5_636f,
        0x84c8_7814,
        0x8cc7_0208,
        0x90be_fffa,
        0xa450_6ceb,
        0xbef9_a3f7,
        0xc671_78f2,
    ];

    let bit_length = (message.len() as u64).wrapping_mul(8);
    let mut padded = message.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_length.to_be_bytes());

    let mut state = INITIAL;
    for chunk in padded.chunks_exact(64) {
        let mut words = [0_u32; 64];
        for (index, bytes) in chunk.chunks_exact(4).enumerate() {
            words[index] = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        }
        for index in 16..64 {
            let s0 = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let s1 = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(s0)
                .wrapping_add(words[index - 7])
                .wrapping_add(s1);
        }
        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = state;
        for index in 0..64 {
            let upper = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let choose = (e & f) ^ ((!e) & g);
            let first = h
                .wrapping_add(upper)
                .wrapping_add(choose)
                .wrapping_add(ROUND[index])
                .wrapping_add(words[index]);
            let lower = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let majority = (a & b) ^ (a & c) ^ (b & c);
            let second = lower.wrapping_add(majority);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(first);
            d = c;
            c = b;
            b = a;
            a = first.wrapping_add(second);
        }
        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }
    let mut output = [0_u8; 32];
    for (chunk, word) in output.chunks_exact_mut(4).zip(state) {
        chunk.copy_from_slice(&word.to_be_bytes());
    }
    output
}
