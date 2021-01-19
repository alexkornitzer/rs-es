// FIXME: This is too permissive, it allows us to build incorrect payloads for ES but it was the
// quickest way to add composite support to what is currently here.
use std::collections::HashMap;

use serde::ser::{SerializeMap, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::bucket::Terms;
use super::{object_to_result, Aggregation, Aggregations, AggregationsResult};
use crate::error::EsError;
use crate::units::JsonVal;

#[derive(Debug)]
pub enum CompositeInner<'a> {
    Terms(Box<Terms<'a>>),
}

impl<'a> Serialize for CompositeInner<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use self::CompositeInner::*;
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Terms(ref t) => map.serialize_entry("terms", t)?,
        };
        map.end()
    }
}

#[derive(Debug)]
pub struct CompositeAggregation<'a> {
    pub after: Option<HashMap<String, Value>>,
    pub size: Option<u64>,
    pub sources: Vec<(&'a str, CompositeInner<'a>)>,
}

impl<'a> Serialize for CompositeAggregation<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut inner = HashMap::with_capacity(self.sources.len());
        for (k, v) in &self.sources {
            inner.insert(k, v);
        }
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("sources", &inner)?;
        if let Some(after) = &self.after {
            map.serialize_entry("after", after)?;
        }
        if let Some(size) = &self.size {
            map.serialize_entry("size", size)?;
        }
        map.end()
    }
}

impl<'a> From<CompositeAggregation<'a>> for Aggregation<'a> {
    fn from(from: CompositeAggregation<'a>) -> Aggregation<'a> {
        Aggregation::Composite(from.into(), None)
    }
}

impl<'a> From<(CompositeAggregation<'a>, Aggregations<'a>)> for Aggregation<'a> {
    fn from(from: (CompositeAggregation<'a>, Aggregations<'a>)) -> Aggregation<'a> {
        Aggregation::Composite(from.0.into(), Some(from.1))
    }
}

macro_rules! from_json_object {
    ($j:ident, $f:expr) => {
        match $j.get($f) {
            Some(val) => match val.as_object() {
                Some(field_val) => {
                    let mut map = HashMap::with_capacity(field_val.len());
                    for (k, v) in field_val {
                        map.insert(k.into(), JsonVal::from(v)?);
                    }
                    map
                }
                None => return_no_field!($f),
            },
            None => return_no_field!($f),
        }
    };
}

macro_rules! from_some_json_object {
    ($j:ident, $f:expr) => {
        match $j.get($f) {
            Some(val) => match val.as_object() {
                Some(field_val) => {
                    let mut map = HashMap::with_capacity(field_val.len());
                    for (k, v) in field_val {
                        map.insert(k.into(), JsonVal::from(v)?);
                    }
                    Some(map)
                }
                None => return_no_field!($f),
            },
            None => None,
        }
    };
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CompositeResult {
    pub key: HashMap<String, JsonVal>,
    pub doc_count: u64,
    pub aggs: Option<AggregationsResult>,
}

impl CompositeResult {
    fn from(from: &Value, aggs: &Option<Aggregations>) -> Result<Self, EsError> {
        Ok(CompositeResult {
            key: from_json_object!(from, "key"),
            doc_count: from_json!(from, "doc_count", as_u64),
            aggs: extract_aggs!(from, aggs),
        })
    }

    add_aggs_ref!();
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CompositeAggregationResult {
    pub after_key: Option<HashMap<String, JsonVal>>,
    pub buckets: Vec<CompositeResult>,
}

impl CompositeAggregationResult {
    pub fn from(from: &Value, aggs: &Option<Aggregations>) -> Result<Self, EsError> {
        Ok(CompositeAggregationResult {
            after_key: from_some_json_object!(from, "after_key"),
            buckets: from_bucket_vector!(from, bucket, CompositeResult::from(bucket, aggs)),
        })
    }
}
