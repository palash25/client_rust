//! Metric type implementations.

pub mod counter;
pub mod exemplar;
pub mod family;
pub mod gauge;
pub mod histogram;
pub mod summary;
pub mod info;

/// A metric that is aware of its Open Metrics metric type.
pub trait TypedMetric {
    const TYPE: MetricType = MetricType::Unknown;
}

#[derive(Clone, Copy)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Info,
    Unknown,
    Summary,
    // Not (yet) supported metric types.
    //
    // GaugeHistogram,
    // StateSet,
}
