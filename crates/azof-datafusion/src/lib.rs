pub mod context;
pub mod parse;

use std::any::Any;
use std::collections::HashSet;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use azof::Lakehouse;
use azof::Projection::{All, Columns};
use azof::{AsOf, AzofError};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream,
};
use object_store::path::Path;
use object_store::ObjectStore;

pub struct AzofTableProvider {
    lakehouse: Arc<Lakehouse>,
    table_name: String,
    as_of: AsOf,
    schema: SchemaRef,
}

impl Clone for AzofTableProvider {
    fn clone(&self) -> Self {
        Self {
            lakehouse: self.lakehouse.clone(),
            table_name: self.table_name.clone(),
            as_of: self.as_of,
            schema: self.schema.clone(),
        }
    }
}

impl Debug for AzofTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AzofTableProvider(table={}, as_of={:?})",
            self.table_name, self.as_of
        )
    }
}

impl AzofTableProvider {
    pub async fn new(
        store_path: Path,
        store: Arc<dyn ObjectStore>,
        table_name: String,
        as_of: AsOf,
    ) -> std::result::Result<Self, AzofError> {
        let lakehouse = Arc::new(Lakehouse::new(store_path, store));

        let schema = Arc::new(
            lakehouse
                .get_schema(&table_name)
                .await?
                .to_arrow_schema(&All)?,
        );

        Ok(Self {
            lakehouse,
            table_name,
            as_of,
            schema,
        })
    }

    pub async fn current(
        store_path: Path,
        store: Arc<dyn ObjectStore>,
        table_name: String,
    ) -> std::result::Result<Self, AzofError> {
        Self::new(store_path, store, table_name, AsOf::Current).await
    }

    pub async fn as_of(
        store_path: Path,
        store: Arc<dyn ObjectStore>,
        table_name: String,
        event_time: chrono::DateTime<chrono::Utc>,
    ) -> std::result::Result<Self, AzofError> {
        Self::new(store_path, store, table_name, AsOf::EventTime(event_time)).await
    }
}

#[async_trait]
impl TableProvider for AzofTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = AzofExec::new(
            self.lakehouse.clone(),
            self.table_name.clone(),
            self.as_of,
            projection.cloned(),
            self.schema.clone(),
        );

        Ok(Arc::new(exec))
    }
}

pub struct AzofExec {
    lakehouse: Arc<Lakehouse>,
    table_name: String,
    as_of: AsOf,
    projected_schema: SchemaRef,
    cache: PlanProperties,
}

impl Debug for AzofExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzofExec")
            .field("table_name", &self.table_name)
            .field("as_of", &self.as_of)
            .field("projected_schema", &self.projected_schema)
            .finish()
    }
}

impl Clone for AzofExec {
    fn clone(&self) -> Self {
        Self {
            lakehouse: self.lakehouse.clone(),
            table_name: self.table_name.clone(),
            as_of: self.as_of,
            projected_schema: self.projected_schema.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl AzofExec {
    pub fn new(
        lakehouse: Arc<Lakehouse>,
        table_name: String,
        as_of: AsOf,
        projection: Option<Vec<usize>>,
        schema: SchemaRef,
    ) -> Self {
        let projected_schema = match &projection {
            Some(indices) => {
                let fields = indices
                    .iter()
                    .map(|i| schema.field(*i).clone())
                    .collect::<Vec<_>>();
                Arc::new(Schema::new(fields))
            }
            None => schema,
        };

        let cache = Self::compute_properties(projected_schema.clone());

        Self {
            lakehouse,
            table_name,
            as_of,
            projected_schema,
            cache,
        }
    }

    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl DisplayAs for AzofExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "AzofExec: table={}, as_of={:?}",
            self.table_name, self.as_of
        )
    }
}

impl ExecutionPlan for AzofExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn name(&self) -> &str {
        "AzofExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let lakehouse = self.lakehouse.clone();
        let table_name = self.table_name.clone();
        let as_of = self.as_of;
        let schema = self.projected_schema.clone();

        let names: HashSet<String> = schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();

        let fut = async move {
            let batch = lakehouse
                .scan(&table_name, as_of, Columns(names))
                .await
                .map_err(|e| {
                    DataFusionError::Execution(format!("Error scanning Azof table: {}", e))
                })?;
            Ok::<Vec<RecordBatch>, DataFusionError>(vec![batch])
        };

        let batches = futures::executor::block_on(fut)?;

        Ok(Box::pin(MemoryStream::try_new(
            batches,
            self.projected_schema.clone(),
            None,
        )?))
    }
}
