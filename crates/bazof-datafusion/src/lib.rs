use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use bazof::AsOf;
use bazof::Lakehouse;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use object_store::path::Path;
use object_store::ObjectStore;
use datafusion::catalog::Session;
use datafusion::physical_plan::PlanProperties;
use datafusion::physical_plan::Partitioning;


pub struct BazofTableProvider {
    lakehouse: Arc<Lakehouse>,
    table_name: String,
    as_of: AsOf,
    schema: SchemaRef,
}

impl Clone for BazofTableProvider {
    fn clone(&self) -> Self {
        Self {
            lakehouse: self.lakehouse.clone(),
            table_name: self.table_name.clone(),
            as_of: self.as_of,
            schema: self.schema.clone(),
        }
    }
}

impl Debug for BazofTableProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BazofTableProvider(table={}, as_of={:?})", self.table_name, self.as_of)
    }
}

impl BazofTableProvider {
    pub fn new(
        store_path: Path, 
        store: Arc<dyn ObjectStore>, 
        table_name: String, 
        as_of: AsOf
    ) -> Result<Self> {
        let lakehouse = Arc::new(Lakehouse::new(store_path, store));
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("event_time", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())), false),
        ]));
        
        Ok(Self {
            lakehouse,
            table_name,
            as_of,
            schema,
        })
    }
    
    pub fn current(
        store_path: Path, 
        store: Arc<dyn ObjectStore>, 
        table_name: String
    ) -> Result<Self> {
        Self::new(store_path, store, table_name, AsOf::Current)
    }
    
    pub fn as_of(
        store_path: Path, 
        store: Arc<dyn ObjectStore>, 
        table_name: String,
        event_time: chrono::DateTime<chrono::Utc>
    ) -> Result<Self> {
        Self::new(store_path, store, table_name, AsOf::EventTime(event_time))
    }
}

#[async_trait]
impl TableProvider for BazofTableProvider {
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
        let exec = BazofExec::new(
            self.lakehouse.clone(),
            self.table_name.clone(),
            self.as_of,
            projection.cloned(),
            self.schema.clone(),
        );
        
        Ok(Arc::new(exec))
    }
}

pub struct BazofExec {
    lakehouse: Arc<Lakehouse>,
    table_name: String,
    as_of: AsOf,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    cache: PlanProperties,
}

impl Debug for BazofExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("BazofExec")
            .field("table_name", &self.table_name)
            .field("as_of", &self.as_of)
            .field("projection", &self.projection)
            .finish()
    }
}

impl Clone for BazofExec {
    fn clone(&self) -> Self {
        Self {
            lakehouse: self.lakehouse.clone(),
            table_name: self.table_name.clone(),
            as_of: self.as_of,
            projected_schema: self.projected_schema.clone(),
            projection: self.projection.clone(),
            cache: self.cache.clone(),
        }
    }
}

impl BazofExec {
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
            projection,
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

impl DisplayAs for BazofExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "BazofExec: table={}, as_of={:?}", self.table_name, self.as_of)
    }
}

impl ExecutionPlan for BazofExec {
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
        "BazofExec"
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
        let projection = self.projection.clone();
        let schema = self.projected_schema.clone();
        
        let fut = async move {
            let batch = lakehouse.scan(&table_name, as_of).await
                .map_err(|e| DataFusionError::Execution(format!("Error scanning Bazof table: {}", e)))?;
            
            let projected_batch = match projection {
                Some(indices) => {
                    let projected_columns = indices
                        .iter()
                        .map(|&i| batch.column(i).clone())
                        .collect::<Vec<_>>();
                    
                    RecordBatch::try_new(
                        schema,
                        projected_columns,
                    )?
                }
                None => {
                    RecordBatch::try_new(
                        schema,
                        batch.columns().to_vec(),
                    )?
                }
            };
            
            Ok::<Vec<RecordBatch>, DataFusionError>(vec![projected_batch])
        };
        
        let batches = futures::executor::block_on(fut)?;
        
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            self.projected_schema.clone(),
            None,
        )?))
    }
}