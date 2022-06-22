//! Module implementing an Open Metrics histogram.
//!
//! See [`Summary`] for details.

use super::{MetricType, TypedMetric};
//use owning_ref::OwningRef;
//use std::iter::{self, once};
use std::sync::{Arc, Mutex};

use quantiles::ckms::CKMS;

/// Open Metrics [`Summary`] to measure distributions of discrete events.
pub struct Summary {
    target_quantile: Vec<f64>,
    target_error: f64,
    max_age_buckets: u64,
    max_age_seconds: u64,
    inner: Arc<Mutex<InnerSummary>>,
}

impl Clone for Summary {
    fn clone(&self) -> Self {
        Summary {
            target_quantile: self.target_quantile.clone(),
            target_error: self.target_error,
            max_age_buckets: self.max_age_buckets,
            max_age_seconds: self.max_age_seconds,
            inner: self.inner.clone(),
        }
    }
}

pub(crate) struct InnerSummary {
    sum: f64,
    count: u64,
    quantile_streams: Vec<CKMS<f64>>,
    // head_stream is like a cursor which carries the index
    // of the stream in the quantile_streams that we want to query
    head_stream: u64,
}

impl Summary {
    pub fn new(max_age_buckets: u64, max_age_seconds: u64, target_quantile: Vec<f64>, target_error: f64) -> Self {
        let mut streams: Vec<CKMS<f64>> = Vec::new();
        for _ in 0..max_age_buckets {
            streams.push(CKMS::new(target_error));
        }

        Summary{
            max_age_buckets,
            max_age_seconds,
            target_quantile,
            target_error,
            inner: Arc::new(Mutex::new(InnerSummary {
                sum: Default::default(),
                count: Default::default(),
                quantile_streams: streams,
                head_stream: 0,
            }))
        }
    }

    pub fn observe(&mut self, v: f64) {
        let mut inner = self.inner.lock().unwrap();
        inner.sum += v;
        inner.count += 1;

        // insert quantiles into all streams/buckets.
        for stream in inner.quantile_streams.iter_mut() {
            stream.insert(v);
        }
    }

    pub fn get(&self) -> (f64, u64, Vec<(f64, f64)>) {
        let inner = self.inner.lock().unwrap();
        let sum = inner.sum;
        let count = inner.count;
        let head = inner.head_stream;
        let mut quantile_values: Vec<(f64, f64)> = Vec::new();

        // TODO: add stream rotation
        for q in self.target_quantile.iter() {
            match inner.quantile_streams[head as usize].query(*q) {
                Some((_, v)) => quantile_values.push((*q, v)),
                None => continue, // TODO fix this
            };
        }
        (sum, count, quantile_values)
    }
}

// TODO: should this type impl Default like Counter?

impl TypedMetric for Summary {
    const TYPE: MetricType = MetricType::Summary;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut summary = Summary::new(5, 10, vec![0.5, 0.9, 0.99], 0.01);
        summary.observe(5.0);
        summary.observe(15.0);
        summary.observe(25.0);

        let (s, c, q) = summary.get();
        assert_eq!(45.0, s);
        assert_eq!(3, c);

        for elem in q.iter() {
            println!("Vec<{}, {}>", elem.0, elem.1);
        }
    }
}