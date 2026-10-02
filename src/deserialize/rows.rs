use crate::deserialize::error::{DriverDeserializationError, DriverRowIterationError};
use crate::deserialize::row_factory::{PyRowFactory, RowBuilder};
use crate::deserialize::value::{PyDeserializeValue, PyDeserializedValue};
use pyo3::{Py, PyAny, PyResult, Python};
use scylla::response::query_result::QueryResult;
use scylla_cql::deserialize::FrameSlice;
use scylla_cql::deserialize::result::RawRowIterator;
use scylla_cql::deserialize::row::ColumnIterator;
use stable_deref_trait::StableDeref;
use std::ops::Deref;
use std::sync::Arc;
use yoke::{Yoke, Yokeable};

/// A page together with the row builder resolved against its columns.
#[derive(Clone)]
pub(crate) enum ResolvedPage {
    Rows {
        query_result: Arc<QueryResult>,
        builder: RowBuilder,
    },
    NonRows(Arc<QueryResult>),
}

impl ResolvedPage {
    pub(crate) fn new(
        py: Python<'_>,
        query_result: Arc<QueryResult>,
        factory: &PyRowFactory,
    ) -> PyResult<Self> {
        let Some(rows) = query_result.deserialized_metadata_and_rows() else {
            return Ok(Self::NonRows(query_result));
        };
        let builder = RowBuilder::resolve(py, factory, rows.metadata().col_specs())?;

        Ok(Self::Rows {
            query_result,
            builder,
        })
    }

    pub(crate) fn query_result(&self) -> &QueryResult {
        match self {
            Self::Rows { query_result, .. } | Self::NonRows(query_result) => query_result,
        }
    }
}

/// Determines how to iterate over query results based on result type.
///
/// Dispatches to either row iteration or handles non-row results.
pub(crate) enum RowsIteratorKind {
    Rows {
        rows: PageRowIterator,
        builder: RowBuilder,
    },
    NonRows,
}

impl RowsIteratorKind {
    pub(crate) fn new(page: ResolvedPage) -> Self {
        match page {
            ResolvedPage::Rows {
                query_result,
                builder,
            } => RowsIteratorKind::Rows {
                rows: PageRowIterator::new(query_result),
                builder,
            },
            ResolvedPage::NonRows(_) => RowsIteratorKind::NonRows,
        }
    }

    /// Switches to the next page, resolving the row builder against its columns.
    pub(crate) fn update(
        &mut self,
        py: Python,
        query_result: Arc<QueryResult>,
        factory: &PyRowFactory,
    ) -> PyResult<()> {
        *self = Self::new(ResolvedPage::new(py, query_result, factory)?);
        Ok(())
    }

    pub(crate) fn next(
        &mut self,
        py: Python<'_>,
    ) -> Option<Result<Py<PyAny>, DriverRowIterationError>> {
        let RowsIteratorKind::Rows { rows, builder } = self else {
            return None;
        };

        rows.next_row(py, builder)
    }
}

/// Iterator over the rows of a single page.
///
/// The iterators borrow directly from the underlying frame, so they are held in
/// a yoke together with the buffer they point into.
pub(crate) struct PageRowIterator {
    yoked: Yoke<PageRows<'static>, QueryResultCart>,
}

impl PageRowIterator {
    /// Only for the `query_result` of a [`ResolvedPage::Rows`].
    fn new(query_result: Arc<QueryResult>) -> Self {
        let yoked = Yoke::attach_to_cart(QueryResultCart(query_result), |cart| {
            let raw_rows_with_metadata = cart
                .deserialized_metadata_and_rows()
                .expect("ResolvedPage::Rows only holds a page that carries rows");
            let frame_slice = FrameSlice::new(raw_rows_with_metadata.raw_rows());

            PageRows {
                rows: RawRowIterator::new(
                    raw_rows_with_metadata.rows_count(),
                    raw_rows_with_metadata.metadata().col_specs(),
                    frame_slice,
                ),
                columns: None,
            }
        });

        Self { yoked }
    }

    fn next_row(
        &mut self,
        py: Python<'_>,
        builder: &RowBuilder,
    ) -> Option<Result<Py<PyAny>, DriverRowIterationError>> {
        if let Err(err) = self.advance()? {
            return Some(Err(DriverRowIterationError::Deserialization(err)));
        }

        let columns = self
            .yoked
            .get()
            .columns
            .clone()
            .expect("advance() stores the column iterator whenever it reports a row");

        Some(builder.build(py, ColumnDeserializer::new(py, columns)))
    }

    fn advance(&mut self) -> Option<Result<(), DriverDeserializationError>> {
        self.yoked.with_mut_return(|page| match page.rows.next()? {
            Ok(columns) => {
                page.columns = Some(columns);
                Some(Ok(()))
            }
            Err(err) => Some(Err(DriverDeserializationError::scylla_decode_failed(err))),
        })
    }
}

/// Stable cart holding deserialized metadata and raw row data.
///
/// This type exists solely to serve as a `StableDeref` cart for `Yoke`.
struct QueryResultCart(Arc<QueryResult>);

impl Deref for QueryResultCart {
    type Target = QueryResult;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl StableDeref for QueryResultCart {}

#[derive(Yokeable)]
pub(crate) struct PageRows<'a> {
    rows: RawRowIterator<'a, 'a>,
    columns: Option<ColumnIterator<'a, 'a>>,
}

/// The columns of a single row, deserialized to Python objects as they are
/// consumed.
pub(crate) struct ColumnDeserializer<'a, 'py> {
    columns: ColumnIterator<'a, 'a>,
    py: Python<'py>,
}

impl<'a, 'py> ColumnDeserializer<'a, 'py> {
    pub(crate) fn new(py: Python<'py>, columns: ColumnIterator<'a, 'a>) -> Self {
        Self { columns, py }
    }
}

impl Iterator for ColumnDeserializer<'_, '_> {
    type Item = Result<PyDeserializedValue, DriverRowIterationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let column = match self.columns.next()? {
            Ok(column) => column,
            Err(err) => {
                return Some(Err(
                    DriverDeserializationError::scylla_decode_failed(err).into()
                ));
            }
        };

        Some(
            PyDeserializedValue::deserialize_py(column.spec.typ(), column.slice, self.py).map_err(
                |err| {
                    err.at_column_name(column.spec.name())
                        .at_column_index(column.index)
                        .into()
                },
            ),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.columns.size_hint()
    }
}

impl ExactSizeIterator for ColumnDeserializer<'_, '_> {}
